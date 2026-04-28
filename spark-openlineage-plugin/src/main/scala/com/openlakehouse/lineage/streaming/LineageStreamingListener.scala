package com.openlakehouse.lineage.streaming

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import com.openlakehouse.lineage.LineageConfig
import com.openlakehouse.lineage.driver.{DatasetRef, EventSink, JobNaming, JobRef, RunContext, RunEventBuilder, RunStatus}

/**
 * Spark `StreamingQueryListener` that turns the three lifecycle hooks of a
 * structured-streaming query into a stream of OpenLineage events.
 *
 * ### Event mapping
 *
 *   - `onQueryStarted`   → `START`     (once per streaming run)
 *   - `onQueryProgress`  → `RUNNING`   (rate-limited by `streamingProgressEveryN`)
 *   - `onQueryTerminated`→ `COMPLETE` or `FAIL`
 *
 * Each streaming query gets its own `runId` derived from Spark's own
 * `QueryStartedEvent.runId` (a UUID). A query restart produces a fresh runId
 * — consistent with OpenLineage's "one run = one continuous execution" model.
 *
 * ### Plan access constraints
 *
 * Unlike `QueryExecutionListener`, the streaming listener does *not* expose
 * the logical plan. We derive dataset identity from the source/sink
 * `description` strings carried on `StreamingQueryProgress`. That's a
 * deliberate compromise — our `StreamingSourceParser` handles the common
 * `FileStreamSource`/`KafkaV2`/`DeltaSource` cases and falls back to a raw
 * string so no emission is ever silently dropped.
 */
final class LineageStreamingListener(
    config: LineageConfig,
    sparkConf: SparkConf,
    eventBuilder: RunEventBuilder,
    sink: EventSink,
    clock: () => Instant = () => Instant.now()
) extends StreamingQueryListener
    with Logging {

  import LineageStreamingListener._

  private val active = new ConcurrentHashMap[UUID, StreamingRun]()

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    if (config.disabled) return
    safely {
      val runUuid = event.runId
      val job     = JobRef(config.namespace, sanitiseJobName(Option(event.name).getOrElse(event.id.toString)))
      val facets  = Map(
        "streamingQueryId"   -> event.id.toString,
        "streamingQueryName" -> Option(event.name).getOrElse(""),
        "startedAt"          -> Option(event.timestamp).getOrElse(""),
        "producer"           -> "spark-openlineage-plugin"
      )
      val ctx = RunContext
        .start(job, now = clock(), executionId = None, facets = facets)
        .copy(runId = runUuid)
      active.put(runUuid, new StreamingRun(ctx))
      emitSafely(ctx)
    }
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    if (config.disabled) return
    safely {
      val p   = event.progress
      val run = active.get(p.runId)
      if (run != null && run.shouldEmit(p.batchId, config.streamingProgressEveryN)) {
        val inputs  = safeRefs(p.sources.map(s => StreamingSourceParser.parseSource(s.description)).toSeq)
        val outputs = Option(p.sink).map(s => StreamingSourceParser.parseSink(s.description)).toSeq
        val facets  = run.ctx.facets ++ progressFacets(p)

        val ctx = run.ctx
          .withInputs(inputs)
          .withOutputs(outputs)
          .withStatus(RunStatus.Running, now = clock())
          .copy(facets = facets)

        // Keep the ctx updated for the terminal event.
        run.update(ctx)
        emitSafely(ctx)
      }
    }
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    if (config.disabled) return
    safely {
      val run = active.remove(event.runId)
      if (run != null) {
        val ctx0 = run.ctx
        val ctx = event.exception match {
          case Some(msg) =>
            ctx0.copy(
              status        = RunStatus.Fail,
              eventTime     = clock(),
              errorMessage  = Some(msg),
              errorClass    = event.errorClassOnException.map(c => c: String).orElse(Some("StreamingQueryException"))
            )
          case None =>
            ctx0.withStatus(RunStatus.Complete, now = clock())
        }
        emitSafely(ctx)
      }
    }
  }

  private def emitSafely(ctx: RunContext): Unit = {
    try {
      val event = eventBuilder.build(ctx)
      sink.send(event)
    } catch {
      case NonFatal(t) =>
        logWarning(s"OpenLineage streaming emit failed for run ${ctx.runId} (${ctx.status.wireValue})", t)
    }
  }

  private def safely(thunk: => Unit): Unit =
    try thunk
    catch {
      case NonFatal(t) =>
        logWarning("OpenLineage streaming listener swallowed an error", t)
    }

  private def safeRefs(refs: Seq[DatasetRef]): Seq[DatasetRef] = DatasetRef.distinctByIdentity(refs)

  private def progressFacets(p: StreamingQueryProgress): Map[String, String] = Map(
    "batchId"               -> p.batchId.toString,
    "batchDurationMs"       -> p.batchDuration.toString,
    "numInputRows"          -> p.numInputRows.toString,
    "inputRowsPerSecond"    -> p.inputRowsPerSecond.toString,
    "processedRowsPerSecond"-> p.processedRowsPerSecond.toString
  )

  private def sanitiseJobName(raw: String): String =
    JobNaming.sanitise(if (raw == null || raw.isEmpty) "streaming-query" else raw)
}

object LineageStreamingListener {

  /** Per-run state. Not exposed outside the listener. */
  private final class StreamingRun(@volatile var ctx: RunContext) {
    private val lastEmittedBatchId = new AtomicLong(-1L)

    def shouldEmit(batchId: Long, everyN: Int): Boolean = {
      val n = math.max(1, everyN)
      val prev = lastEmittedBatchId.get()
      val eligible = batchId == 0L || (batchId - prev) >= n
      if (eligible) lastEmittedBatchId.set(batchId)
      eligible
    }

    def update(next: RunContext): Unit = { ctx = next }
  }
}
