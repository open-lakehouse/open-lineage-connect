package com.openlakehouse.lineage

import org.apache.spark.SparkConf

/**
 * Typed view over the `spark.openlineage.*` configuration keys.
 *
 * Defaults here are intentionally conservative: the plugin fails open
 * (never blocks or fails the Spark job) unless the operator explicitly
 * opts into stricter behavior.
 */
final case class LineageConfig(
    serviceUrl: Option[String],
    namespace: String,
    authToken: Option[String],
    jobNameOverride: Option[String],
    emitTaskMetrics: Boolean,
    emitColumnLineage: Boolean,
    streamingProgressEveryN: Int,
    queueSize: Int,
    batchFlushMs: Long,
    disabled: Boolean,
    failOpen: Boolean
)

object LineageConfig {

  val ServiceUrlKey                = "spark.openlineage.serviceUrl"
  val NamespaceKey                 = "spark.openlineage.namespace"
  val AuthTokenKey                 = "spark.openlineage.authToken"
  val JobNameKey                   = "spark.openlineage.jobName"
  val EmitTaskMetricsKey           = "spark.openlineage.emit.taskMetrics"
  val EmitColumnLineageKey         = "spark.openlineage.emit.columnLineage"
  val StreamingProgressEveryNKey   = "spark.openlineage.emit.streaming.progressEveryN"
  val QueueSizeKey                 = "spark.openlineage.queueSize"
  val BatchFlushMsKey              = "spark.openlineage.batchFlushMs"
  val DisabledKey                  = "spark.openlineage.disabled"
  val FailOpenKey                  = "spark.openlineage.failOpen"

  def fromSparkConf(conf: SparkConf): LineageConfig = LineageConfig(
    serviceUrl              = Option(conf.get(ServiceUrlKey, null)).filter(_.nonEmpty),
    namespace               = conf.get(NamespaceKey, "default"),
    authToken               = Option(conf.get(AuthTokenKey, null)).filter(_.nonEmpty),
    jobNameOverride         = Option(conf.get(JobNameKey, null)).filter(_.nonEmpty),
    emitTaskMetrics         = conf.getBoolean(EmitTaskMetricsKey, defaultValue = true),
    emitColumnLineage       = conf.getBoolean(EmitColumnLineageKey, defaultValue = false),
    streamingProgressEveryN = conf.getInt(StreamingProgressEveryNKey, 1).max(1),
    queueSize               = conf.getInt(QueueSizeKey, 1024).max(1),
    batchFlushMs            = conf.getLong(BatchFlushMsKey, 250L).max(1L),
    disabled                = conf.getBoolean(DisabledKey, defaultValue = false),
    failOpen                = conf.getBoolean(FailOpenKey, defaultValue = true)
  )
}
