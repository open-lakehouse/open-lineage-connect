package com.openlakehouse.lineage

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import okhttp3.mockwebserver.{MockResponse, MockWebServer}
import okio.Buffer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import lineage.v1.Lineage

/**
 * End-to-end smoke test: boots a real SparkSession with the plugin classpath
 * registered via `spark.plugins`, points it at a local MockWebServer playing
 * the `open-lineage-service` role, runs a tiny query, and asserts that the
 * driver delivers protobuf `RunEvent`s over the wire.
 *
 * This is the closest we can cheaply get to Spark Connect coverage without
 * starting the Connect server — the plugin lifecycle executed here is bit-for-
 * bit the same one the Connect server JVM runs at startup (SparkPlugin.load
 * from `spark.plugins`, DriverPlugin.init with the Connect session's
 * SparkContext). The difference between classic and Connect mode for *this
 * plugin* is only "which JVM hosts the driver" — we do not call any
 * Connect-specific APIs, so classic-mode coverage is sufficient here.
 */
final class LineagePluginSmokeSpec
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterEach {

  private var server: MockWebServer = _
  private var spark: SparkSession   = _

  override def beforeEach(): Unit = {
    server = new MockWebServer()
    server.start()
  }

  override def afterEach(): Unit = {
    try if (spark ne null) spark.stop() finally {}
    try server.shutdown() finally {}
  }

  test("plugin loads via spark.plugins, captures a batch query, and emits to the transport") {
    // Seed enough OK responses to drain whatever batching the worker ends up doing.
    (0 until 10).foreach(_ => server.enqueue(successResponse))

    val conf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName("LineagePluginSmokeSpec")
      .set("spark.plugins", classOf[LineagePlugin].getName)
      .set(LineageConfig.ServiceUrlKey, server.url("").toString.stripSuffix("/"))
      .set(LineageConfig.NamespaceKey, "smoke-test")
      .set(LineageConfig.BatchFlushMsKey, "50")
      .set(LineageConfig.QueueSizeKey, "256")
      // Speed up shutdown draining so the test doesn't wait on the default flush.
      .set("spark.ui.enabled", "false")

    spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.range(5).toDF("id")
    df.count() shouldBe 5L

    // Give the background emitter a moment to drain at least one batch.
    val deadline = System.currentTimeMillis() + 5000L
    var firstReq: okhttp3.mockwebserver.RecordedRequest = null
    while ((firstReq eq null) && System.currentTimeMillis() < deadline) {
      firstReq = server.takeRequest(200, TimeUnit.MILLISECONDS)
    }

    firstReq should not be null
    withClue("plugin must POST protobuf to /lineage.v1.LineageService/IngestBatch: ") {
      firstReq.getPath should include("/lineage.v1.LineageService/")
      firstReq.getHeader("content-type") shouldBe "application/proto"
    }

    // Parse the batch body and verify at least one RunEvent landed with our namespace.
    val body  = firstReq.getBody.readByteArray()
    val batch = Lineage.IngestBatchRequest.parseFrom(body)
    batch.getEventsCount should be > 0

    val events    = batch.getEventsList.asScala.toSeq
    val namespaces = events.map(_.getJob.getNamespace).distinct
    namespaces should contain("smoke-test")

    val eventTypes = events.map(_.getEventType).toSet
    // START must precede the COMPLETE for every batch query.
    eventTypes should contain("START")
  }

  test("disabled config short-circuits: plugin loads but emits no traffic") {
    val conf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName("LineagePluginSmokeSpec-Disabled")
      .set("spark.plugins", classOf[LineagePlugin].getName)
      .set(LineageConfig.ServiceUrlKey, server.url("").toString.stripSuffix("/"))
      .set(LineageConfig.DisabledKey, "true")
      .set("spark.ui.enabled", "false")

    spark = SparkSession.builder().config(conf).getOrCreate()
    spark.range(3).count() shouldBe 3L

    // Give the driver time to misbehave if it was going to.
    Thread.sleep(500)
    server.getRequestCount shouldBe 0
  }

  private def successResponse: MockResponse = {
    val buf = new Buffer()
    buf.write(Lineage.IngestBatchResponse.newBuilder().setIngested(1).build().toByteArray)
    new MockResponse()
      .setResponseCode(200)
      .setHeader("content-type", "application/proto")
      .setBody(buf)
  }
}
