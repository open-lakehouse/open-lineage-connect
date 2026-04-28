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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.openlakehouse.lineage.LineageConfig
import com.openlakehouse.lineage.driver.{
  ColumnLineageExtractor,
  ColumnLineageFacet,
  DatasetRef,
  EventSink,
  JobNaming,
  JobRef,
  RunContext,
  RunEventBuilder,
  RunStatus
}

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
    clock: () => Instant = () => Instant.now(),
    /**
     * Optional plan lookup for column lineage. The default reaches into the
     * active SparkSession's StreamingQueryManager and pulls
     * `IncrementalExecution.analyzed` reflectively (the StreamExecution
     * subclass that exposes `lastExecution` lives in
     * `org.apache.spark.sql.execution.streaming` and is not part of the
     * stable public API). Tests can supply a deterministic lookup.
     */
    planLookup: UUID => Option[LogicalPlan] =
      LineageStreamingListener.defaultPlanLookup
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

        val columnLineage =
          if (config.emitColumnLineage)
            planLookup(p.runId).map(safeExtractColumnLineage).getOrElse(Map.empty)
          else Map.empty[(String, String), ColumnLineageFacet]

        val ctx = run.ctx
          .withInputs(inputs)
          .withOutputs(outputs)
          .withStatus(RunStatus.Running, now = clock())
          .copy(facets = facets)
          .withColumnLineage(columnLineage)

        // Keep the ctx updated for the terminal event.
        run.update(ctx)
        emitSafely(ctx)
      }
    }
  }

  private def safeExtractColumnLineage(plan: LogicalPlan): Map[(String, String), ColumnLineageFacet] = {
    try ColumnLineageExtractor.extract(plan)
    catch {
      case NonFatal(t) =>
        logWarning("OpenLineage streaming column-lineage extraction failed; continuing with empty map", t)
        Map.empty
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

  /**
   * Default plan lookup: ask the active SparkSession for the streaming query
   * with the given runId, then reach for `lastExecution.analyzed` via
   * reflection. `StreamExecution` is in
   * `org.apache.spark.sql.execution.streaming` which Spark treats as
   * implementation-only — reflection lets us extract the plan without a hard
   * compile-time dependency on those internals.
   *
   * Returns `None` for any failure path: no active session, no query for the
   * runId, query type lacks `lastExecution`, plan still null. Callers must
   * tolerate `None` gracefully.
   */
  def defaultPlanLookup(runId: UUID): Option[LogicalPlan] = {
    try {
      val session = org.apache.spark.sql.SparkSession.getActiveSession
        .orElse(org.apache.spark.sql.SparkSession.getDefaultSession)
      session.flatMap { s =>
        val q = s.streams.get(runId)
        if (q == null) None else extractAnalyzedReflectively(q)
      }
    } catch { case NonFatal(_) => None }
  }

  private def extractAnalyzedReflectively(query: AnyRef): Option[LogicalPlan] = {
    try {
      val lastExec = query.getClass.getMethods
        .find(m => m.getName == "lastExecution" && m.getParameterCount == 0)
        .map(_.invoke(query))
        .orNull
      if (lastExec == null) None
      else {
        val analyzed = lastExec.getClass.getMethods
          .find(m => m.getName == "analyzed" && m.getParameterCount == 0)
          .map(_.invoke(lastExec))
          .orNull
        analyzed match {
          case p: LogicalPlan => Some(p)
          case _              => None
        }
      }
    } catch { case NonFatal(_) => None }
  }

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
