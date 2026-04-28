package com.openlakehouse.lineage.driver

import com.openlakehouse.lineage.SparkTestBase
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer

/**
 * Integration tests for `QueryPlanVisitor.extractSinks`.
 *
 * Spark rewrites write commands during execution, so we capture the analyzed
 * plan via a `QueryExecutionListener` installed against the test session —
 * that's also the exact hook the real plugin will use in production.
 */
final class QueryPlanVisitorSinksSpec
    extends AnyFunSuite
    with Matchers
    with SparkTestBase
    with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(25, Millis))

  /** Listener that records the analyzed plan of every completed query. */
  private final class CapturingListener extends QueryExecutionListener {
    val captured: ArrayBuffer[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] =
      ArrayBuffer.empty
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
      captured.synchronized { captured += qe.analyzed; () }
    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
    def latest: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
      captured.synchronized(captured.last)
  }

  private def withListener[T](body: CapturingListener => T): T = {
    val listener = new CapturingListener
    spark.listenerManager.register(listener)
    try body(listener)
    finally spark.listenerManager.unregister(listener)
  }

  test("parquet write surfaces an InsertIntoHadoopFsRelationCommand sink") {
    val ss = spark; import ss.implicits._
    val dst = tmpDir.resolve("out.parquet").toString

    withListener { l =>
      Seq((1, "a"), (2, "b")).toDF("id", "name").write.mode("overwrite").parquet(dst)
      eventually { l.captured.size should be >= 1 }
      val sinks = QueryPlanVisitor.extractSinks(l.latest)
      sinks should have size 1
      val s = sinks.head
      s.namespace should (startWith("file://") or equal("file://"))
      s.name     should include(dst)
      s.sourceType.toLowerCase should include("parquet")
      s.facets   should contain key ("saveMode")
    }
  }

  test("saveAsTable with a session catalog target surfaces a catalog-identity sink") {
    val ss = spark; import ss.implicits._
    spark.sql("CREATE DATABASE IF NOT EXISTS sinks_db")
    spark.sql("DROP TABLE IF EXISTS sinks_db.orders")
    try {
      withListener { l =>
        Seq((1, 10.0), (2, 20.0)).toDF("id", "amount")
          .write.mode("overwrite").saveAsTable("sinks_db.orders")

        // Walk backwards through captured plans to find the one that contains a
        // sink — intermediate analysis steps may produce plans without one.
        val sinks = firstNonEmptySinks(l)

        sinks should have size 1
        val s = sinks.head
        s.namespace shouldBe "sinks_db"
        s.name      shouldBe "orders"
        s.facets.get("catalog") shouldBe Some("session")
      }
    } finally {
      spark.sql("DROP TABLE IF EXISTS sinks_db.orders")
      spark.sql("DROP DATABASE IF EXISTS sinks_db")
    }
  }

  test("INSERT INTO an existing table surfaces a V2WriteCommand OR V1 sink") {
    val ss = spark; import ss.implicits._
    spark.sql("CREATE DATABASE IF NOT EXISTS sinks_db")
    spark.sql("DROP TABLE IF EXISTS sinks_db.events")
    Seq((1, "x")).toDF("id", "tag").write.mode("overwrite").saveAsTable("sinks_db.events")

    try {
      withListener { l =>
        spark.sql("INSERT INTO sinks_db.events VALUES (2, 'y'), (3, 'z')")

        val sinks = firstNonEmptySinks(l)

        sinks should have size 1
        val s = sinks.head
        s.name shouldBe "events"
        // Whether it resolves to V2 (AppendData) or V1 (InsertIntoHadoopFs) depends on
        // Spark's planning; both should yield sinks_db.events.
        s.namespace should (equal("sinks_db") or endWith("sinks_db"))
      }
    } finally {
      spark.sql("DROP TABLE IF EXISTS sinks_db.events")
      spark.sql("DROP DATABASE IF EXISTS sinks_db")
    }
  }

  test("CTAS surfaces a CreateDataSourceTableAsSelectCommand sink") {
    val ss = spark; import ss.implicits._
    spark.sql("CREATE DATABASE IF NOT EXISTS sinks_db")
    spark.sql("DROP TABLE IF EXISTS sinks_db.rollup")
    // seed source
    Seq((1, 10.0), (2, 20.0)).toDF("id", "amount")
      .write.mode("overwrite").saveAsTable("sinks_db.src_for_ctas")

    try {
      withListener { l =>
        spark.sql("CREATE TABLE sinks_db.rollup USING PARQUET AS SELECT id, amount * 2 AS doubled FROM sinks_db.src_for_ctas")

        val sinks = firstNonEmptySinks(l)

        sinks should have size 1
        val s = sinks.head
        s.name      shouldBe "rollup"
        s.namespace shouldBe "sinks_db"
        s.facets.get("ctas") orElse s.facets.get("writeKind") should not be empty
      }
    } finally {
      spark.sql("DROP TABLE IF EXISTS sinks_db.rollup")
      spark.sql("DROP TABLE IF EXISTS sinks_db.src_for_ctas")
      spark.sql("DROP DATABASE IF EXISTS sinks_db")
    }
  }

  test("a pure read plan has no sinks") {
    val ss = spark; import ss.implicits._
    val src = tmpDir.resolve("noSink.parquet").toString
    Seq((1, "a")).toDF("id", "v").write.mode("overwrite").parquet(src)

    val plan  = spark.read.parquet(src).queryExecution.analyzed
    val sinks = QueryPlanVisitor.extractSinks(plan)
    sinks shouldBe empty
  }

  test("reflective sink matcher is inert when no connector commands are on the classpath") {
    // Without Delta/Iceberg/Hudi on the test classpath this should be a no-op
    // against any plan we throw at it — primarily a regression test that the
    // reflective scan does not crash the main extractor.
    val ss = spark; import ss.implicits._
    val src = tmpDir.resolve("rf-src.parquet").toString
    val dst = tmpDir.resolve("rf-dst.parquet").toString
    Seq((1, "a")).toDF("id", "v").write.mode("overwrite").parquet(src)

    withListener { l =>
      spark.read.parquet(src).write.mode("overwrite").parquet(dst)
      val sinks = firstNonEmptySinks(l)
      // Exactly one sink, from the V1 write command — reflective layer added nothing extra.
      sinks should have size 1
      sinks.head.name should include(dst)
    }
  }

  /**
   * Walk captured plans newest-first and return the first non-empty extractSinks
   * result. Spark emits multiple plans per write (incl. empty rewrites); the
   * final/analyzed one usually carries the sink.
   */
  private def firstNonEmptySinks(l: CapturingListener): Seq[DatasetRef] =
    l.captured.reverseIterator
      .map(QueryPlanVisitor.extractSinks)
      .collectFirst { case refs if refs.nonEmpty => refs }
      .getOrElse(Seq.empty)
}
