package com.openlakehouse.lineage.driver

import com.openlakehouse.lineage.SparkTestBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for `QueryPlanVisitor.extractSources`.
 *
 * Each test builds a real `LogicalPlan` through the DataFrame API and asserts
 * that the visitor extracts the expected `DatasetRef`(s). The tests run against
 * a single shared `SparkSession(local[1])` for suite-scoped performance.
 */
final class QueryPlanVisitorSpec extends AnyFunSuite with Matchers with SparkTestBase {

  test("extractSources on a literal LocalRelation returns nothing") {
    val ss = spark; import ss.implicits._
    val df = Seq(1, 2, 3).toDF("n")
    val sources = QueryPlanVisitor.extractSources(df.queryExecution.analyzed)
    sources shouldBe empty
  }

  test("parquet path read surfaces a HadoopFsRelation-keyed source") {
    val ss = spark; import ss.implicits._
    val src = tmpDir.resolve("src.parquet").toString
    Seq((1, "a"), (2, "b")).toDF("id", "name").write.mode("overwrite").parquet(src)

    val df      = spark.read.parquet(src)
    val sources = QueryPlanVisitor.extractSources(df.queryExecution.analyzed)

    sources should have size 1
    val dr = sources.head
    dr.namespace should (startWith("file://") or equal("file://"))
    dr.name     should include(src)
    dr.sourceType.toLowerCase should include("parquet")
  }

  test("json path read keeps the source-type label distinct from parquet") {
    val ss = spark; import ss.implicits._
    val src = tmpDir.resolve("src.json").toString
    Seq("""{"id":1}""", """{"id":2}""").toDS().write.mode("overwrite").text(src)

    val df      = spark.read.json(src)
    val sources = QueryPlanVisitor.extractSources(df.queryExecution.analyzed)

    sources should have size 1
    sources.head.sourceType.toLowerCase should include("json")
  }

  test("session-local catalog table read surfaces the catalog identifier") {
    val ss = spark; import ss.implicits._
    spark.sql("CREATE DATABASE IF NOT EXISTS lineage_test")
    spark.sql("DROP TABLE IF EXISTS lineage_test.customers")
    Seq((1, "Ada"), (2, "Linus"))
      .toDF("id", "name")
      .write
      .mode("overwrite")
      .saveAsTable("lineage_test.customers")

    try {
      val df      = spark.table("lineage_test.customers")
      val sources = QueryPlanVisitor.extractSources(df.queryExecution.analyzed)

      sources should have size 1
      val dr = sources.head
      dr.namespace shouldBe "lineage_test"
      dr.name      shouldBe "customers"
      dr.sourceType.toLowerCase should (include("parquet") or equal("parquet") or equal("hive"))
    } finally {
      spark.sql("DROP TABLE IF EXISTS lineage_test.customers")
      spark.sql("DROP DATABASE IF EXISTS lineage_test")
    }
  }

  test("join across two sources yields two deduplicated DatasetRefs") {
    val ss = spark; import ss.implicits._
    val a = tmpDir.resolve("a.parquet").toString
    val b = tmpDir.resolve("b.parquet").toString
    Seq((1, "a1"), (2, "a2")).toDF("id", "va").write.mode("overwrite").parquet(a)
    Seq((1, "b1"), (2, "b2")).toDF("id", "vb").write.mode("overwrite").parquet(b)

    val joined  = spark.read.parquet(a).join(spark.read.parquet(b), "id")
    val sources = QueryPlanVisitor.extractSources(joined.queryExecution.analyzed)

    sources should have size 2
    val names = sources.map(_.name)
    names.exists(_.contains(a)) shouldBe true
    names.exists(_.contains(b)) shouldBe true
  }

  test("self-join over the same path returns a single deduplicated source") {
    val ss = spark; import ss.implicits._
    val src = tmpDir.resolve("self.parquet").toString
    Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "v").write.mode("overwrite").parquet(src)

    val left    = spark.read.parquet(src).alias("l")
    val right   = spark.read.parquet(src).alias("r")
    val joined  = left.join(right, left("id") === right("id"))
    val sources = QueryPlanVisitor.extractSources(joined.queryExecution.analyzed)

    sources should have size 1
    sources.head.name should include(src)
  }

  test("distinctByIdentity keeps first occurrence") {
    val a = DatasetRef("ns", "t", "parquet")
    val b = DatasetRef("ns", "t", "json") // same identity, different sourceType
    val c = DatasetRef("ns", "other", "parquet")
    DatasetRef.distinctByIdentity(Seq(a, b, c)) shouldBe Seq(a, c)
  }
}
