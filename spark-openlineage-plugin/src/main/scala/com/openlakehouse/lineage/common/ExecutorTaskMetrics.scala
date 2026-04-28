package com.openlakehouse.lineage.common

/**
 * Per-task metrics shipped from an executor to the driver via
 * `PluginContext.send`. Spark serializes the payload with its standard RPC
 * codec (Java serialization), so this must be a plain `Serializable` case class
 * with no cyclic references to Spark internals.
 *
 * Keep the schema tight: every field we add grows every task-level message,
 * and these messages fire O(tasks) during a query. Current fields are limited
 * to workload-sizing metrics that users can correlate with Spark UI counters.
 *
 * `executionId` is the SQL execution id read from the task's local properties
 * (`SQLExecution.EXECUTION_ID_KEY = "spark.sql.execution.id"`). It's the key
 * the driver uses to attribute task metrics to the right query run. If it's
 * missing (RDD-only jobs, ad-hoc actions), the message is still delivered but
 * won't be attached to a lineage run — that's intentional.
 */
final case class ExecutorTaskMetrics(
    executionId: Option[Long],
    stageId: Int,
    stageAttemptNumber: Int,
    taskAttemptId: Long,
    executorId: String,
    host: String,
    recordsRead: Long,
    bytesRead: Long,
    recordsWritten: Long,
    bytesWritten: Long,
    executorRunTimeMs: Long,
    executorCpuTimeNs: Long,
    jvmGcTimeMs: Long,
    peakExecutionMemoryBytes: Long,
    failed: Boolean,
    failureReason: Option[String]
) extends Serializable

object ExecutorTaskMetrics {
  /** SQL execution id property key. Stable across Spark 3.x → 4.x. */
  val ExecutionIdPropertyKey: String = "spark.sql.execution.id"
}
