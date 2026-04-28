package com.openlakehouse.lineage.driver

import java.time.Instant

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import com.openlakehouse.lineage.LineageConfig

/**
 * Spark `QueryExecutionListener` that turns each completed batch query into
 * a pair of OpenLineage events (`START` + `COMPLETE` on success, `START` + `FAIL`
 * on exception) and pushes them into the configured `EventSink`.
 *
 * The listener is the narrow bridge between Spark's Catalyst APIs and the
 * plugin's protobuf event model. It:
 *
 *   - Never throws back into Spark. Any extractor or encoder failure is logged
 *     and swallowed — the `failOpen` config is not optional in practice; we'd
 *     rather miss lineage than crash a user job.
 *   - Builds one `RunContext` per query, so `runId` is stable across the START
 *     and COMPLETE/FAIL events of the same query execution.
 *   - Emits the START event *before* the terminal event, preserving the
 *     expected OpenLineage event ordering even though both are generated in
 *     the same listener callback.
 *
 * Streaming queries are handled by a separate `LineageStreamingListener`;
 * this class only sees batch queries.
 */
private[lineage] final class LineageQueryListener(
    config: LineageConfig,
    sparkConf: SparkConf,
    eventBuilder: RunEventBuilder,
    sink: EventSink,
    metrics: TaskMetricsAggregator = new TaskMetricsAggregator,
    clock: () => Instant = () => Instant.now()
) extends QueryExecutionListener
    with Logging {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    observe(qe, funcName, durationNs = Some(durationNs), error = None)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    observe(qe, funcName, durationNs = None, error = Some(exception))
  }

  private def observe(
      qe: QueryExecution,
      funcName: String,
      durationNs: Option[Long],
      error: Option[Throwable]
  ): Unit = {
    if (config.disabled) return
    try {
      val plan       = qe.analyzed
      val job        = JobNaming.derive(config, sparkConf, plan, description = Some(funcName))
      val executionId = try Some(qe.id) catch { case NonFatal(_) => None }

      val baseFacets = Map.newBuilder[String, String]
      baseFacets += "funcName"        -> funcName
      baseFacets += "producer"        -> "spark-openlineage-plugin"
      durationNs.foreach(d => baseFacets += "durationNs" -> d.toString)

      val startCtx = RunContext
        .start(job, now = clock(), executionId = executionId, facets = baseFacets.result())
        .withInputs(safeExtract(QueryPlanVisitor.extractSources(plan), "sources"))
        .withOutputs(safeExtract(QueryPlanVisitor.extractSinks(plan), "sinks"))

      // START event.
      emitSafely(startCtx)

      // Terminal event: COMPLETE on success, FAIL on exception. Executor
      // task metrics (if any) are snapshotted now and attached as facets on
      // the terminal event only — the START event precedes task execution
      // so it has no meaningful metrics to report.
      val terminalBase = error match {
        case Some(t) => startCtx.withError(t, now = clock())
        case None    => startCtx.withStatus(RunStatus.Complete, now = clock())
      }
      val terminal = executionId
        .flatMap(metrics.snapshot)
        .map(snap => terminalBase.withFacets(snap.asFacets))
        .getOrElse(terminalBase)

      emitSafely(terminal)
    } catch {
      case NonFatal(t) =>
        logWarning("OpenLineage listener failed to observe query execution; swallowing", t)
    }
  }

  private def safeExtract(extract: => Seq[DatasetRef], label: String): Seq[DatasetRef] = {
    try extract
    catch {
      case NonFatal(t) =>
        logWarning(s"OpenLineage $label extraction failed; continuing with empty list", t)
        Seq.empty
    }
  }

  private def emitSafely(ctx: RunContext): Unit = {
    try {
      val event = eventBuilder.build(ctx)
      sink.send(event)
    } catch {
      case NonFatal(t) =>
        logWarning(s"OpenLineage emit failed for run ${ctx.runId} (${ctx.status.wireValue})", t)
    }
  }
}
