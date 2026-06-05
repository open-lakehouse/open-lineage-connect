package com.openlakehouse.lineage

import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class LineageConfigSpec extends AnyFunSuite with Matchers {

  test("fromSparkConf returns conservative defaults") {
    val cfg = LineageConfig.fromSparkConf(new SparkConf(loadDefaults = false))

    cfg.serviceUrl              shouldBe empty
    cfg.namespace               shouldBe "default"
    cfg.jobNameOverride         shouldBe empty
    cfg.emitTaskMetrics         shouldBe true
    cfg.emitColumnLineage       shouldBe false
    cfg.streamingProgressEveryN shouldBe 1
    cfg.queueSize               shouldBe 1024
    cfg.batchFlushMs            shouldBe 250L
    cfg.disabled                shouldBe false
    cfg.failOpen                shouldBe true
  }

  test("fromSparkConf reads explicit settings") {
    val conf = new SparkConf(loadDefaults = false)
      .set(LineageConfig.ServiceUrlKey, "http://lineage:8090")
      .set(LineageConfig.NamespaceKey, "warehouse-prod")
      .set(LineageConfig.JobNameKey, "etl.customers.hourly")
      .set(LineageConfig.EmitTaskMetricsKey, "false")
      .set(LineageConfig.EmitColumnLineageKey, "true")
      .set(LineageConfig.StreamingProgressEveryNKey, "5")
      .set(LineageConfig.QueueSizeKey, "4096")
      .set(LineageConfig.BatchFlushMsKey, "500")
      .set(LineageConfig.DisabledKey, "true")
      .set(LineageConfig.FailOpenKey, "false")

    val cfg = LineageConfig.fromSparkConf(conf)

    cfg.serviceUrl              shouldBe Some("http://lineage:8090")
    cfg.namespace               shouldBe "warehouse-prod"
    cfg.jobNameOverride         shouldBe Some("etl.customers.hourly")
    cfg.emitTaskMetrics         shouldBe false
    cfg.emitColumnLineage       shouldBe true
    cfg.streamingProgressEveryN shouldBe 5
    cfg.queueSize               shouldBe 4096
    cfg.batchFlushMs            shouldBe 500L
    cfg.disabled                shouldBe true
    cfg.failOpen                shouldBe false
  }

  test("streamingProgressEveryN and queueSize clamp to a minimum of 1") {
    val conf = new SparkConf(loadDefaults = false)
      .set(LineageConfig.StreamingProgressEveryNKey, "0")
      .set(LineageConfig.QueueSizeKey, "-4")
      .set(LineageConfig.BatchFlushMsKey, "0")

    val cfg = LineageConfig.fromSparkConf(conf)

    cfg.streamingProgressEveryN shouldBe 1
    cfg.queueSize               shouldBe 1
    cfg.batchFlushMs            shouldBe 1L
  }

  test("empty serviceUrl is treated as unset") {
    val conf = new SparkConf(loadDefaults = false).set(LineageConfig.ServiceUrlKey, "")
    LineageConfig.fromSparkConf(conf).serviceUrl shouldBe empty
  }

  test("run and job facets default to empty maps") {
    val cfg = LineageConfig.fromSparkConf(new SparkConf(loadDefaults = false))
    cfg.runFacets shouldBe empty
    cfg.jobFacets shouldBe empty
  }

  test("facets.run.* and facets.job.* are parsed under their respective prefixes") {
    val conf = new SparkConf(loadDefaults = false)
      .set(LineageConfig.RunFacetPrefix + "owner", "data-eng")
      .set(LineageConfig.RunFacetPrefix + "pipeline", "nightly")
      .set(LineageConfig.JobFacetPrefix + "team", "platform")

    val cfg = LineageConfig.fromSparkConf(conf)

    cfg.runFacets shouldBe Map("owner" -> "data-eng", "pipeline" -> "nightly")
    cfg.jobFacets shouldBe Map("team" -> "platform")
  }

  test("facet values that are empty are dropped") {
    val conf = new SparkConf(loadDefaults = false)
      .set(LineageConfig.RunFacetPrefix + "present", "yes")
      .set(LineageConfig.RunFacetPrefix + "blank", "")

    val cfg = LineageConfig.fromSparkConf(conf)

    cfg.runFacets shouldBe Map("present" -> "yes")
  }

  test("tls config defaults to disabled with PKCS12 store types") {
    val cfg = LineageConfig.fromSparkConf(new SparkConf(loadDefaults = false))
    cfg.tls.enabled shouldBe false
    cfg.tls.trustStorePath shouldBe empty
    cfg.tls.keyStorePath shouldBe empty
    cfg.tls.trustStoreType shouldBe "PKCS12"
    cfg.tls.keyStoreType shouldBe "PKCS12"
  }

  test("tls config parses trust store, key store, and types") {
    val conf = new SparkConf(loadDefaults = false)
      .set(LineageConfig.TlsTrustStorePathKey, "/etc/ssl/trust.jks")
      .set(LineageConfig.TlsTrustStorePasswordKey, "trustpw")
      .set(LineageConfig.TlsTrustStoreTypeKey, "JKS")
      .set(LineageConfig.TlsKeyStorePathKey, "/etc/ssl/client.p12")
      .set(LineageConfig.TlsKeyStorePasswordKey, "keypw")

    val cfg = LineageConfig.fromSparkConf(conf)

    cfg.tls.enabled shouldBe true
    cfg.tls.trustStorePath shouldBe Some("/etc/ssl/trust.jks")
    cfg.tls.trustStorePassword shouldBe Some("trustpw")
    cfg.tls.trustStoreType shouldBe "JKS"
    cfg.tls.keyStorePath shouldBe Some("/etc/ssl/client.p12")
    cfg.tls.keyStorePassword shouldBe Some("keypw")
    cfg.tls.keyStoreType shouldBe "PKCS12"
  }

  test("retry and rate-limit defaults") {
    val cfg = LineageConfig.fromSparkConf(new SparkConf(loadDefaults = false))
    cfg.retryMaxRetries shouldBe 2
    cfg.retryBackoffMs shouldBe 200L
    cfg.retryJitterFactor shouldBe 0.5
    cfg.rateLimitMinIntervalMs shouldBe 0L
  }

  test("retry and rate-limit explicit settings with clamping") {
    val conf = new SparkConf(loadDefaults = false)
      .set(LineageConfig.RetryMaxRetriesKey, "-3")        // clamps to 0
      .set(LineageConfig.RetryBackoffMsKey, "-10")        // clamps to 0
      .set(LineageConfig.RetryJitterFactorKey, "2.5")     // clamps to 1.0
      .set(LineageConfig.RateLimitMinIntervalMsKey, "750")

    val cfg = LineageConfig.fromSparkConf(conf)

    cfg.retryMaxRetries shouldBe 0
    cfg.retryBackoffMs shouldBe 0L
    cfg.retryJitterFactor shouldBe 1.0
    cfg.rateLimitMinIntervalMs shouldBe 750L
  }

  test("jitter factor below zero clamps to zero") {
    val conf = new SparkConf(loadDefaults = false)
      .set(LineageConfig.RetryJitterFactorKey, "-1.0")
    LineageConfig.fromSparkConf(conf).retryJitterFactor shouldBe 0.0
  }
}
