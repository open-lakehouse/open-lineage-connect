package com.openlakehouse.lineage.streaming

import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingTestHelpers
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.openlakehouse.lineage.LineageConfig
import com.openlakehouse.lineage.driver.{EventSink, RunEventBuilder}

/**
 * Unit-level spec for `LineageStreamingListener`. We feed the listener the
 * three `StreamingQueryListener` events directly, bypassing a real streaming
 * query. This keeps the test fast and deterministic while still exercising
 * the full source/sink parsing + event-building pipeline end-to-end.
 */
final class LineageStreamingListenerSpec extends AnyFunSuite with Matchers {

  private val config = LineageConfig(
    serviceUrl              = None,
    namespace               = "stream-test",
    jobNameOverride         = None,
    emitTaskMetrics         = false,
    emitColumnLineage       = false,
    streamingProgressEveryN = 1,
    queueSize               = 16,
    batchFlushMs            = 1L,
    disabled                = false,
    failOpen                = true
  )

  private def newListener(): (EventSink.InMemory, LineageStreamingListener) = {
    val sink = new EventSink.InMemory
    val listener = new LineageStreamingListener(
      config       = config,
      sparkConf    = new SparkConf(),
      eventBuilder = new RunEventBuilder(),
      sink         = sink
    )
    (sink, listener)
  }

  private def progressEvent(
      runId: UUID,
      batchId: Long,
      sources: Seq[String],
      sinkDesc: Option[String]
  ) = {
    val sourceArr = sources.map(StreamingTestHelpers.newSourceProgress).toArray
    val sink      = StreamingTestHelpers.newSinkProgress(sinkDesc.getOrElse("noop"))
    StreamingTestHelpers.newProgressEvent(StreamingTestHelpers.newProgress(
      id        = UUID.randomUUID(),
      runId     = runId,
      name      = "test",
      timestamp = "2026-01-01T00:00:00Z",
      batchId   = batchId,
      sources   = sourceArr,
      sink      = sink
    ))
  }

  test("QueryStartedEvent emits a START event with the streaming runId") {
    val (sink, listener) = newListener()
    val qid = UUID.randomUUID()
    val rid = UUID.randomUUID()

    listener.onQueryStarted(new QueryStartedEvent(qid, rid, "my-stream", "2026-01-01T00:00:00Z"))

    sink.size shouldBe 1
    val ev = sink.events.head
    ev.getEventType shouldBe "START"
    ev.getRun.getRunId shouldBe rid.toString
    ev.getJob.getNamespace shouldBe "stream-test"
    ev.getJob.getName shouldBe "my-stream"
    val facets = ev.getRun.getFacets
    facets.getFieldsOrThrow("streamingQueryId").getStringValue shouldBe qid.toString
  }

  test("QueryProgressEvent emits RUNNING with input/output datasets and batchId facet") {
    val (sink, listener) = newListener()
    val rid = UUID.randomUUID()
    listener.onQueryStarted(new QueryStartedEvent(UUID.randomUUID(), rid, "s", "ts"))

    val p = progressEvent(
      runId    = rid,
      batchId  = 0L,
      sources  = Seq("KafkaV2[Subscribe[orders]]"),
      sinkDesc = Some("FileSink[s3a://lakehouse/curated/orders]")
    )
    listener.onQueryProgress(p)

    sink.size shouldBe 2
    val running = sink.events.last
    running.getEventType shouldBe "RUNNING"
    running.getInputsList.asScala.map(_.getName) should contain("orders")
    running.getOutputsList.asScala.map(_.getName) should contain("/curated/orders")
    running.getRun.getFacets.getFieldsOrThrow("batchId").getStringValue shouldBe "0"
    running.getRun.getFacets.getFieldsOrThrow("numInputRows").getStringValue shouldBe "100"
  }

  test("streamingProgressEveryN rate-limits progress events after batch 0") {
    val rateLimitedConfig = config.copy(streamingProgressEveryN = 3)
    val sinkBuf = new EventSink.InMemory
    val listener = new LineageStreamingListener(
      config       = rateLimitedConfig,
      sparkConf    = new SparkConf(),
      eventBuilder = new RunEventBuilder(),
      sink         = sinkBuf
    )
    val rid = UUID.randomUUID()
    listener.onQueryStarted(new QueryStartedEvent(UUID.randomUUID(), rid, "s", "ts"))

    // batchId 0 always emits; 1 and 2 are filtered; 3 emits; 4/5 filtered.
    Seq(0L, 1L, 2L, 3L, 4L, 5L).foreach { bid =>
      listener.onQueryProgress(progressEvent(rid, bid, Seq("KafkaV2[Subscribe[t]]"), Some("FileSink[file:/out]")))
    }
    // 1 START + 2 RUNNING (batches 0 and 3) = 3 events total
    sinkBuf.size shouldBe 3
    val runningBatches = sinkBuf.events.filter(_.getEventType == "RUNNING")
      .map(_.getRun.getFacets.getFieldsOrThrow("batchId").getStringValue)
    runningBatches should contain theSameElementsAs Seq("0", "3")
  }

  test("QueryTerminatedEvent with no exception emits COMPLETE") {
    val (sink, listener) = newListener()
    val rid = UUID.randomUUID()
    listener.onQueryStarted(new QueryStartedEvent(UUID.randomUUID(), rid, "s", "ts"))
    listener.onQueryTerminated(StreamingTestHelpers.newTerminatedEvent(UUID.randomUUID(), rid))

    sink.size shouldBe 2
    sink.events.last.getEventType shouldBe "COMPLETE"
    sink.events.last.getRun.getRunId shouldBe rid.toString
  }

  test("QueryTerminatedEvent with exception emits FAIL and carries errorMessage") {
    val (sink, listener) = newListener()
    val rid = UUID.randomUUID()
    listener.onQueryStarted(new QueryStartedEvent(UUID.randomUUID(), rid, "s", "ts"))
    listener.onQueryTerminated(StreamingTestHelpers.newTerminatedEvent(UUID.randomUUID(), rid, Some("boom")))

    val fail = sink.events.find(_.getEventType == "FAIL").get
    fail.getRun.getFacets.getFieldsOrThrow("errorMessage").getStringValue shouldBe "boom"
  }

  test("disabled config short-circuits all lifecycle events") {
    val disabledConfig = config.copy(disabled = true)
    val sinkBuf = new EventSink.InMemory
    val listener = new LineageStreamingListener(
      config       = disabledConfig,
      sparkConf    = new SparkConf(),
      eventBuilder = new RunEventBuilder(),
      sink         = sinkBuf
    )
    val rid = UUID.randomUUID()
    listener.onQueryStarted(new QueryStartedEvent(UUID.randomUUID(), rid, "s", "ts"))
    listener.onQueryProgress(progressEvent(rid, 0L, Seq("KafkaV2[Subscribe[t]]"), Some("FileSink[file:/out]")))
    listener.onQueryTerminated(StreamingTestHelpers.newTerminatedEvent(UUID.randomUUID(), rid))
    sinkBuf.size shouldBe 0
  }

  test("progress events for unknown runIds are ignored") {
    val (sink, listener) = newListener()
    // Never call onQueryStarted — listener has no registered run.
    listener.onQueryProgress(progressEvent(UUID.randomUUID(), 0L, Seq("KafkaV2[Subscribe[t]]"), Some("FileSink[file:/out]")))
    sink.size shouldBe 0
  }
}
