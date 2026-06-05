package com.openlakehouse.lineage.driver

import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.openlakehouse.lineage.LineageConfig
import com.openlakehouse.lineage.transport.ConnectRpcEventSink

/**
 * Unit tests for `LineageDriverPlugin.buildSink` — the policy that selects
 * between the inert `EventSink.Noop` and the real `ConnectRpcEventSink`
 * depending on whether `spark.openlineage.serviceUrl` is configured.
 */
final class LineageDriverPluginSpec extends AnyFunSuite with Matchers {

  private def configFrom(pairs: (String, String)*): LineageConfig = {
    val conf = new SparkConf(loadDefaults = false)
    pairs.foreach { case (k, v) => conf.set(k, v) }
    LineageConfig.fromSparkConf(conf)
  }

  test("buildSink returns the Noop sink when serviceUrl is unset") {
    LineageDriverPlugin.buildSink(configFrom()) shouldBe EventSink.Noop
  }

  test("buildSink returns the Noop sink when serviceUrl is empty") {
    LineageDriverPlugin.buildSink(configFrom(LineageConfig.ServiceUrlKey -> "")) shouldBe EventSink.Noop
  }

  test("buildSink returns a real ConnectRpcEventSink when serviceUrl is set") {
    val sink = LineageDriverPlugin.buildSink(
      configFrom(
        LineageConfig.ServiceUrlKey   -> "http://localhost:8090",
        LineageConfig.QueueSizeKey    -> "8",
        LineageConfig.BatchFlushMsKey -> "10"
      )
    )
    try {
      sink shouldBe a[ConnectRpcEventSink]
    } finally {
      // The ConnectRpcEventSink spins up a background worker thread; close it
      // so the test doesn't leak a daemon thread between runs.
      sink.close()
    }
  }
}
