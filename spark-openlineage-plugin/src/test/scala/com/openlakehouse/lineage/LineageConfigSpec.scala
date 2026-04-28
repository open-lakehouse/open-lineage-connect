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
}
