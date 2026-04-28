package com.openlakehouse.lineage.driver

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.openlakehouse.lineage.{LineageConfig, SparkTestBase}
import com.openlakehouse.lineage.common.ExecutorTaskMetrics

/**
 * Drives real Spark queries through `LineageQueryListener` and verifies the
 * emitted events. This is the narrowest integration seam between Catalyst
 * analysis and the OpenLineage event model.
 *
 * We attach the listener directly to `spark.listenerManager` here rather than
 * going through `LineageDriverPlugin.init` so we can assert on ordering
 * without racing the plugin's deferred registration path.
 */
final class LineageQueryListenerSpec extends AnyFunSuite with SparkTestBase with Matchers {

  private def baseConfig: LineageConfig = LineageConfig(
    serviceUrl              = None,
    namespace               = "test",
    jobNameOverride         = None,
    emitTaskMetrics         = false,
    emitColumnLineage       = false,
    streamingProgressEveryN = 1,
    queueSize               = 1,
    batchFlushMs            = 1L,
    disabled                = false,
    failOpen                = true
  )

  private def attachListener(
      cfg: LineageConfig = baseConfig,
      aggregator: TaskMetricsAggregator = new TaskMetricsAggregator
  ): (EventSink.InMemory, LineageQueryListener) = {
    val sink     = new EventSink.InMemory
    val listener = new LineageQueryListener(
      config       = cfg,
      sparkConf    = spark.sparkContext.getConf,
      eventBuilder = new RunEventBuilder(),
      sink         = sink,
      metrics      = aggregator
    )
    spark.listenerManager.register(listener)
    (sink, listener)
  }

  test("batch parquet write emits START followed by COMPLETE with shared runId") {
    val (sink, listener) = attachListener()

    try {
      val ss = spark
      import ss.implicits._
      val in  = tmpDir.resolve("in.parquet").toString
      val out = tmpDir.resolve("out.parquet").toString
      Seq((1, "a"), (2, "b")).toDF("i", "s").write.mode("overwrite").parquet(in)

      spark.read.parquet(in).write.mode("overwrite").parquet(out)

      eventually {
        val events = sink.events
        withClue(s"captured event types=${events.map(_.getEventType).mkString("[",",","]")}") {
          events.size should be >= 2
        }

        // Per-query each call produces a (START, COMPLETE) pair, so find one
        // matching pair that shares a runId. This is more robust than assuming
        // the first write only contributes one pair.
        val byRun = events.groupBy(_.getRun.getRunId)
        val matchedPair = byRun.values.find { group =>
          group.map(_.getEventType).toSet == Set("START", "COMPLETE")
        }.getOrElse(fail(s"no runId produced START+COMPLETE pair, got $byRun"))

        val start    = matchedPair.find(_.getEventType == "START").get
        val complete = matchedPair.find(_.getEventType == "COMPLETE").get

        start.getJob.getNamespace shouldBe "test"
        start.getJob.getName should not be empty
        start.getRun.getRunId shouldBe complete.getRun.getRunId

        val outputs = events
          .filter(_.getEventType == "COMPLETE")
          .flatMap(_.getOutputsList.asScala)
          .map(_.getName)
        withClue(s"all COMPLETE outputs=$outputs") {
          outputs should contain(out)
        }
      }
    } finally spark.listenerManager.unregister(listener)
  }

  test("runtime failure emits FAIL event with errorClass facet") {
    val (sink, listener) = attachListener()
    try {
      // `assert_true(false)` passes analysis but fails during physical execution
      // — which is the only failure mode QueryExecutionListener.onFailure sees.
      // Read.parquet of a missing path would fail at *analysis*, before the
      // listener has a chance to fire.
      an[Exception] should be thrownBy {
        spark.sql("SELECT assert_true(false)").collect()
      }

      eventually {
        val events = sink.events
        withClue(s"captured event types=${events.map(_.getEventType)}") {
          events.map(_.getEventType) should contain("FAIL")
        }

        val fail   = events.find(_.getEventType == "FAIL").get
        val facets = fail.getRun.getFacets
        facets.getFieldsOrThrow("errorClass").getStringValue should not be empty
      }
    } finally spark.listenerManager.unregister(listener)
  }

  test("task metrics from the aggregator are merged into COMPLETE event facets") {
    val aggregator = new TaskMetricsAggregator

    // Primer listener: registered BEFORE our LineageQueryListener so it fires
    // first in Spark's listener bus. It intercepts the executionId of the
    // current query and pre-seeds the aggregator with a synthetic metric
    // payload as though an executor had just shipped it. This simulates the
    // executor→driver round-trip without actually running a cluster.
    val primer = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        val execId = try qe.id catch { case _: Throwable => -1L }
        aggregator.record(ExecutorTaskMetrics(
          executionId              = Some(execId),
          stageId                  = 0,
          stageAttemptNumber       = 0,
          taskAttemptId            = 0L,
          executorId               = "primer",
          host                     = "localhost",
          recordsRead              = 0L,
          bytesRead                = 0L,
          recordsWritten           = 42L,
          bytesWritten             = 4200L,
          executorRunTimeMs        = 150L,
          executorCpuTimeNs        = 1500000L,
          jvmGcTimeMs              = 3L,
          peakExecutionMemoryBytes = 7777L,
          failed                   = false,
          failureReason            = None
        ))
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
    }
    spark.listenerManager.register(primer)

    val (sink, listener) = attachListener(aggregator = aggregator)

    try {
      val ss = spark
      import ss.implicits._
      val out = tmpDir.resolve("metrics-out").toString
      Seq((1, "a"), (2, "b")).toDF("i", "s").write.mode("overwrite").parquet(out)

      eventually {
        val complete = sink.events.find(_.getEventType == "COMPLETE")
          .getOrElse(fail(s"no COMPLETE event, got ${sink.events.map(_.getEventType)}"))
        val facets = complete.getRun.getFacets
        // Core metric facets merged from the aggregator snapshot.
        facets.getFieldsOrThrow("recordsWritten").getStringValue shouldBe "42"
        facets.getFieldsOrThrow("bytesWritten").getStringValue shouldBe "4200"
        facets.getFieldsOrThrow("taskCount").getStringValue shouldBe "1"
        facets.getFieldsOrThrow("failedTaskCount").getStringValue shouldBe "0"
      }
    } finally {
      spark.listenerManager.unregister(listener)
      spark.listenerManager.unregister(primer)
    }
  }

  test("emitColumnLineage=true populates the typed ColumnLineageDatasetFacet on outputs") {
    val (sink, listener) = attachListener(baseConfig.copy(emitColumnLineage = true))
    try {
      val ss = spark
      import ss.implicits._
      val in  = tmpDir.resolve("cl_in.parquet").toString
      val out = tmpDir.resolve("cl_out.parquet").toString
      Seq((1, "ada"), (2, "lin")).toDF("id", "name").write.mode("overwrite").parquet(in)

      spark.read.parquet(in).select($"id", $"name".as("user_name"))
        .write.mode("overwrite").parquet(out)

      eventually {
        val withColumnLineage = sink.events
          .filter(_.getEventType == "START")
          .flatMap(_.getOutputsList.asScala.toList)
          .filter(_.getName == out)

        withClue(s"captured outputs=${withColumnLineage.map(_.getName)}") {
          withColumnLineage should not be empty
        }

        val o = withColumnLineage.head
        o.hasColumnLineage shouldBe true
        val fields = o.getColumnLineage.getFieldsMap
        fields.keySet().asScala should (contain("id") and contain("user_name"))
      }
    } finally spark.listenerManager.unregister(listener)
  }

  test("emitColumnLineage=false leaves the typed facet unset") {
    val (sink, listener) = attachListener()
    try {
      val ss = spark
      import ss.implicits._
      val in  = tmpDir.resolve("cl_off_in.parquet").toString
      val out = tmpDir.resolve("cl_off_out.parquet").toString
      Seq((1, "x")).toDF("id", "v").write.mode("overwrite").parquet(in)

      spark.read.parquet(in).write.mode("overwrite").parquet(out)

      eventually {
        val outputs = sink.events
          .flatMap(_.getOutputsList.asScala.toList)
          .filter(_.getName == out)
        withClue(s"captured outputs=${outputs.map(_.getName)}") {
          outputs should not be empty
        }
        outputs.foreach(o => o.hasColumnLineage shouldBe false)
      }
    } finally spark.listenerManager.unregister(listener)
  }

  test("disabled config short-circuits without emitting anything") {
    val (sink, listener) = attachListener(baseConfig.copy(disabled = true))
    try {
      val ss = spark
      import ss.implicits._
      Seq(1, 2, 3).toDF("x").count()

      // Give the listener bus a brief chance to run; it should stay empty.
      Thread.sleep(250L)
      sink.size shouldBe 0
    } finally spark.listenerManager.unregister(listener)
  }

  /**
   * QueryExecutionListener callbacks run on a background listener-bus thread,
   * so we poll for up to ~2s before giving up. Matches the pattern used by
   * Spark's own tests for QueryExecutionListener.
   */
  private def eventually[T](block: => T): T = {
    val maxMs    = 2000L
    val stepMs   = 25L
    val deadline = System.currentTimeMillis() + maxMs
    var lastErr: Throwable = null
    while (System.currentTimeMillis() < deadline) {
      try return block
      catch {
        case t: Throwable =>
          lastErr = t
          Thread.sleep(stepMs)
      }
    }
    if (lastErr != null) throw lastErr else block
  }
}
