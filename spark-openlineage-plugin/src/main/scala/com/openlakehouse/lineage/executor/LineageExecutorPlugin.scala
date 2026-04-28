package com.openlakehouse.lineage.executor

import java.util.{Map => JMap}

import scala.util.control.NonFatal

import org.apache.spark.{TaskContext, TaskFailedReason}
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

import com.openlakehouse.lineage.LineageConfig
import com.openlakehouse.lineage.common.ExecutorTaskMetrics

/**
 * Executor half of the lineage plugin.
 *
 * Responsibilities:
 *   - Read `extraConf` shipped from the driver in [[LineageConfig]] form.
 *   - On task completion, capture an [[ExecutorTaskMetrics]] payload and send
 *     it to the driver via `PluginContext.send`.
 *
 * No plan introspection lives here — `LogicalPlan` doesn't exist on the
 * executor JVM. We emit only numeric counters that the driver correlates back
 * to the query run via `executionId`.
 *
 * Error safety: `send` is best-effort. A failure to ship metrics never affects
 * task status (we just log and move on). This matches the "fail open" policy
 * documented in the plugin's config.
 */
final class LineageExecutorPlugin extends ExecutorPlugin with Logging {

  @volatile private var ctx: PluginContext      = _
  @volatile private var enabled: Boolean        = false
  @volatile private var metricsEnabled: Boolean = false

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    this.ctx = ctx
    val disabled = java.lang.Boolean.parseBoolean(
      Option(extraConf.get(LineageConfig.DisabledKey)).getOrElse("false")
    )
    this.enabled = !disabled
    this.metricsEnabled = java.lang.Boolean.parseBoolean(
      Option(extraConf.get(LineageConfig.EmitTaskMetricsKey)).getOrElse("true")
    )

    if (enabled) {
      logInfo(s"OpenLineage executor plugin initialized (taskMetrics=$metricsEnabled)")
    } else {
      logInfo("OpenLineage executor plugin loaded in disabled mode")
    }
  }

  override def onTaskSucceeded(): Unit = sendMetrics(failed = false, failureReason = None)

  override def onTaskFailed(failureReason: TaskFailedReason): Unit =
    sendMetrics(failed = true, failureReason = Option(failureReason).map(_.toErrorString))

  private def sendMetrics(failed: Boolean, failureReason: Option[String]): Unit = {
    if (!enabled || !metricsEnabled) return
    val c = ctx
    if (c == null) return

    // TaskContext.get() returns null outside the task runner — e.g. in local
    // mode during driver-side bookkeeping callbacks — so we null-guard before
    // we try to snapshot. Returning `None` here turns the metrics call into a
    // no-op rather than logging every driver-side invocation as a failure.
    val tcOpt = Option(TaskContext.get())
    if (tcOpt.isEmpty) return

    try {
      val payload = capture(tcOpt.get, failed, failureReason)
      c.send(payload)
    } catch {
      case NonFatal(t) =>
        // Purposely INFO not WARN: transient failures are expected during
        // executor decommissioning; this is not an actionable alert.
        logInfo(s"OpenLineage failed to send task metrics to driver: ${t.getMessage}")
    }
  }

  /**
   * Snapshot the supplied `TaskContext.taskMetrics()` into an immutable
   * payload. The caller is responsible for supplying a non-null `tc`; see
   * `sendMetrics` for the null-guard that wraps this.
   *
   * `taskMetrics()` aggregates the current task's metrics incrementally — by
   * the time we're called from `onTaskSucceeded`/`onTaskFailed` all counters
   * reflect final values.
   */
  private def capture(
      tc: TaskContext,
      failed: Boolean,
      failureReason: Option[String]
  ): ExecutorTaskMetrics = {
    val tm = tc.taskMetrics()
    val inputMetrics  = tm.inputMetrics
    val outputMetrics = tm.outputMetrics

    val executionId =
      try Option(tc.getLocalProperty(ExecutorTaskMetrics.ExecutionIdPropertyKey))
        .filter(_.nonEmpty)
        .flatMap(s => scala.util.Try(s.toLong).toOption)
      catch { case NonFatal(_) => None }

    ExecutorTaskMetrics(
      executionId              = executionId,
      stageId                  = tc.stageId(),
      stageAttemptNumber       = tc.stageAttemptNumber(),
      taskAttemptId            = tc.taskAttemptId(),
      executorId               = Option(ctx).map(_.executorID()).getOrElse("unknown"),
      host                     = Option(ctx).map(_.hostname()).getOrElse("unknown"),
      recordsRead              = inputMetrics.recordsRead,
      bytesRead                = inputMetrics.bytesRead,
      recordsWritten           = outputMetrics.recordsWritten,
      bytesWritten             = outputMetrics.bytesWritten,
      executorRunTimeMs        = tm.executorRunTime,
      executorCpuTimeNs        = tm.executorCpuTime,
      jvmGcTimeMs              = tm.jvmGCTime,
      peakExecutionMemoryBytes = tm.peakExecutionMemory,
      failed                   = failed,
      failureReason            = failureReason
    )
  }

  override def shutdown(): Unit = {
    logDebug("OpenLineage executor plugin shutting down")
  }
}
