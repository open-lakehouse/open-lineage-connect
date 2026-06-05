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
    authToken: Option[String] = None,
    jobNameOverride: Option[String],
    emitTaskMetrics: Boolean,
    emitColumnLineage: Boolean,
    streamingProgressEveryN: Int,
    queueSize: Int,
    batchFlushMs: Long,
    disabled: Boolean,
    failOpen: Boolean,
    /**
     * Operator-supplied facets injected onto every emitted run's `Run.facets`,
     * sourced from `spark.openlineage.facets.run.<key>=<value>`. Built-in run
     * facets (funcName, producer, task metrics, ...) take precedence on key
     * collisions.
     */
    runFacets: Map[String, String] = Map.empty,
    /**
     * Operator-supplied facets injected onto every emitted run's `Job.facets`,
     * sourced from `spark.openlineage.facets.job.<key>=<value>`.
     */
    jobFacets: Map[String, String] = Map.empty,
    /** TLS / mTLS transport material for an `https://` `serviceUrl`. */
    tls: TlsConfig = TlsConfig(),
    /** Max retry attempts for a retriable ingest failure (0 disables retries). */
    retryMaxRetries: Int = 2,
    /** Base backoff (ms) for the first retry; doubles each subsequent attempt. */
    retryBackoffMs: Long = 200L,
    /**
     * Symmetric jitter factor in `[0, 1]` applied to the exponential backoff.
     * `0` is fixed backoff; `0.5` (default) spreads each delay over
     * `[0.5x, 1.5x]` to avoid synchronized retry storms.
     */
    retryJitterFactor: Double = 0.5,
    /**
     * Minimum interval (ms) the emitter enforces between successive flushes to
     * the lineage endpoint. `0` (default) disables rate limiting.
     */
    rateLimitMinIntervalMs: Long = 0L
)

/**
 * TLS transport configuration for the ConnectRPC client. All fields are
 * optional; when [[enabled]] is false the client falls back to OkHttp's
 * default JVM trust store (plain `https://` still works, it just uses the
 * platform trust anchors).
 *
 *   - A trust store overrides which server certificates the client accepts —
 *     use it to pin a private CA / self-signed lineage-service cert.
 *   - A key store supplies a client certificate for **mutual TLS** (mTLS),
 *     where the lineage service authenticates the Spark driver in turn.
 */
final case class TlsConfig(
    trustStorePath: Option[String] = None,
    trustStorePassword: Option[String] = None,
    trustStoreType: String = "PKCS12",
    keyStorePath: Option[String] = None,
    keyStorePassword: Option[String] = None,
    keyStoreType: String = "PKCS12"
) {
  /** True when any custom trust or key material is configured. */
  def enabled: Boolean = trustStorePath.isDefined || keyStorePath.isDefined
}

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

  /** Prefix for operator-supplied run facets: `spark.openlineage.facets.run.<key>`. */
  val RunFacetPrefix               = "spark.openlineage.facets.run."
  /** Prefix for operator-supplied job facets: `spark.openlineage.facets.job.<key>`. */
  val JobFacetPrefix               = "spark.openlineage.facets.job."

  val TlsTrustStorePathKey         = "spark.openlineage.tls.trustStorePath"
  val TlsTrustStorePasswordKey     = "spark.openlineage.tls.trustStorePassword"
  val TlsTrustStoreTypeKey         = "spark.openlineage.tls.trustStoreType"
  val TlsKeyStorePathKey           = "spark.openlineage.tls.keyStorePath"
  val TlsKeyStorePasswordKey       = "spark.openlineage.tls.keyStorePassword"
  val TlsKeyStoreTypeKey           = "spark.openlineage.tls.keyStoreType"

  val RetryMaxRetriesKey           = "spark.openlineage.retry.maxRetries"
  val RetryBackoffMsKey            = "spark.openlineage.retry.backoffMs"
  val RetryJitterFactorKey         = "spark.openlineage.retry.jitterFactor"
  val RateLimitMinIntervalMsKey    = "spark.openlineage.rateLimit.minIntervalMs"

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
    failOpen                = conf.getBoolean(FailOpenKey, defaultValue = true),
    runFacets               = facetsWithPrefix(conf, RunFacetPrefix),
    jobFacets               = facetsWithPrefix(conf, JobFacetPrefix),
    tls                     = tlsFromConf(conf),
    retryMaxRetries         = conf.getInt(RetryMaxRetriesKey, 2).max(0),
    retryBackoffMs          = conf.getLong(RetryBackoffMsKey, 200L).max(0L),
    retryJitterFactor       = conf.getDouble(RetryJitterFactorKey, 0.5).max(0.0).min(1.0),
    rateLimitMinIntervalMs  = conf.getLong(RateLimitMinIntervalMsKey, 0L).max(0L)
  )

  private def tlsFromConf(conf: SparkConf): TlsConfig = TlsConfig(
    trustStorePath     = Option(conf.get(TlsTrustStorePathKey, null)).filter(_.nonEmpty),
    trustStorePassword = Option(conf.get(TlsTrustStorePasswordKey, null)).filter(_.nonEmpty),
    trustStoreType     = conf.get(TlsTrustStoreTypeKey, "PKCS12"),
    keyStorePath       = Option(conf.get(TlsKeyStorePathKey, null)).filter(_.nonEmpty),
    keyStorePassword   = Option(conf.get(TlsKeyStorePasswordKey, null)).filter(_.nonEmpty),
    keyStoreType       = conf.get(TlsKeyStoreTypeKey, "PKCS12")
  )

  /**
   * Collect every `<prefix><key>=<value>` pair into a `key -> value` map.
   * Empty keys (e.g. the bare prefix) and empty values are dropped so a
   * misconfigured conf can never inject a blank facet.
   */
  private def facetsWithPrefix(conf: SparkConf, prefix: String): Map[String, String] =
    conf
      .getAllWithPrefix(prefix)
      .iterator
      .filter { case (k, v) => k.nonEmpty && v != null && v.nonEmpty }
      .toMap
}
