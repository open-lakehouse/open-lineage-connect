package com.openlakehouse.lineage.transport

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import okhttp3.mockwebserver.{MockResponse, MockWebServer}
import okio.Buffer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import lineage.v1.Lineage

/**
 * End-to-end exercise of the emitter pipeline:
 *   producer → bounded queue → worker thread → OkHttp → MockWebServer.
 *
 * These tests intentionally push the emitter against its backpressure, retry,
 * and shutdown guarantees rather than just the happy path.
 */
final class ConnectRpcEventSinkSpec extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  private var server: MockWebServer = _
  private var sink: ConnectRpcEventSink = _

  override def beforeEach(): Unit = {
    server = new MockWebServer()
    server.start()
  }

  override def afterEach(): Unit = {
    if (sink != null) {
      try sink.close() catch { case _: Throwable => () }
      sink = null
    }
    server.shutdown()
  }

  private def newSink(
      queueCapacity: Int    = 128,
      batchMaxEvents: Int   = 32,
      batchFlushMs: Long    = 50L,
      maxRetries: Int       = 0,
      retryBackoffMs: Long  = 10L
  ): ConnectRpcEventSink = {
    val transport = new ConnectRpcClient(
      baseUrl = server.url("/").toString.stripSuffix("/"),
      okHttp  = ConnectRpcClient.defaultOkHttp(connectTimeoutMs = 500L, readTimeoutMs = 1500L)
    )
    sink = new ConnectRpcEventSink(
      client         = new LineageServiceClient(transport),
      queueCapacity  = queueCapacity,
      batchMaxEvents = batchMaxEvents,
      batchFlushMs   = batchFlushMs,
      shutdownDrainMs = 2000L,
      maxRetries     = maxRetries,
      retryBackoffMs = retryBackoffMs
    )
    sink
  }

  private def successResponse(ingested: Int): MockResponse = {
    val resp = Lineage.IngestBatchResponse.newBuilder().setIngested(ingested).build()
    new MockResponse()
      .setResponseCode(200)
      .addHeader("Content-Type", ConnectRpcClient.ProtoContentType)
      .setBody(new Buffer().write(resp.toByteArray))
  }

  private def runEvent(runId: String): Lineage.RunEvent =
    Lineage.RunEvent.newBuilder()
      .setEventType("START")
      .setProducer("t")
      .setSchemaUrl("t://s")
      .setRun(Lineage.Run.newBuilder().setRunId(runId))
      .setJob(Lineage.Job.newBuilder().setNamespace("ns").setName("j"))
      .build()

  test("all produced events are delivered across one or more batched POSTs") {
    // Batching is an optimization, not a correctness guarantee: depending on
    // scheduler timing the worker may flush a partial batch before the
    // producer finishes enqueueing. We enqueue enough responses to cover the
    // worst case (one POST per event) and assert on the *total* delivered.
    (1 to 5).foreach(_ => server.enqueue(successResponse(ingested = 1)))
    val s = newSink(batchFlushMs = 25L, batchMaxEvents = 32)

    (1 to 5).foreach(i => s.send(runEvent(s"r-$i")))

    // Drain requests until we've accounted for every event.
    val deadline = System.currentTimeMillis() + 3000L
    val collected = scala.collection.mutable.ListBuffer.empty[String]
    while (collected.size < 5 && System.currentTimeMillis() < deadline) {
      val req = server.takeRequest(500, TimeUnit.MILLISECONDS)
      if (req != null) {
        req.getPath shouldBe LineageServiceClient.IngestBatchPath
        req.getHeader("Content-Type") shouldBe ConnectRpcClient.ProtoContentType
        val payload = Lineage.IngestBatchRequest.parseFrom(req.getBody.readByteArray())
        collected ++= payload.getEventsList.asScala.map(_.getRun.getRunId)
      }
    }
    collected.toList should contain theSameElementsAs Seq("r-1", "r-2", "r-3", "r-4", "r-5")
  }

  test("batch honours batchMaxEvents by splitting into multiple POSTs") {
    server.enqueue(successResponse(ingested = 2))
    server.enqueue(successResponse(ingested = 2))
    server.enqueue(successResponse(ingested = 1))

    val s = newSink(batchMaxEvents = 2, batchFlushMs = 30L)

    (1 to 5).foreach(i => s.send(runEvent(s"r-$i")))

    val reqs = (0 until 3).map(_ => server.takeRequest(3, TimeUnit.SECONDS))
    reqs.foreach(_ should not be null)

    val flatEventIds = reqs.flatMap { r =>
      Lineage.IngestBatchRequest
        .parseFrom(r.getBody.readByteArray())
        .getEventsList.asScala.map(_.getRun.getRunId)
    }
    flatEventIds should contain theSameElementsAs Seq("r-1", "r-2", "r-3", "r-4", "r-5")
  }

  test("queue overflow drops events and increments the counter") {
    // Don't enqueue a response — the worker will block on the first network call,
    // which gives us a deterministic chance to saturate the queue.
    server.enqueue(new MockResponse().setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE))

    val s = newSink(queueCapacity = 3, batchMaxEvents = 1, batchFlushMs = 5L)

    // Let the worker grab the first item and start the blocked HTTP call.
    s.send(runEvent("r-0"))
    Thread.sleep(150L)

    // Now flood: queue holds 3, worker is blocked, every extra send should drop.
    (1 to 10).foreach(i => s.send(runEvent(s"r-$i")))

    s.droppedCount should be >= 5L
  }

  test("retries a retriable 503 then succeeds") {
    server.enqueue(new MockResponse().setResponseCode(503).setBody("unavailable"))
    server.enqueue(successResponse(ingested = 1))

    val s = newSink(maxRetries = 2, retryBackoffMs = 20L, batchFlushMs = 30L)
    s.send(runEvent("r-1"))

    // Two HTTP attempts expected.
    server.takeRequest(3, TimeUnit.SECONDS) should not be null
    server.takeRequest(3, TimeUnit.SECONDS) should not be null

    // Give the worker a moment to update counters after the success.
    Thread.sleep(150L)
    s.sentCount shouldBe 1L
    s.failedCount shouldBe 0L
  }

  test("non-retriable 400 is logged and not retried") {
    server.enqueue(new MockResponse().setResponseCode(400).setBody("bad"))

    val s = newSink(maxRetries = 3, retryBackoffMs = 10L, batchFlushMs = 30L)
    s.send(runEvent("r-bad"))

    server.takeRequest(3, TimeUnit.SECONDS) should not be null
    // Exactly one attempt should have hit the server.
    Thread.sleep(150L)
    server.getRequestCount shouldBe 1
    s.failedCount shouldBe 1L
  }

  test("close() drains pending events through the worker before returning") {
    server.enqueue(successResponse(ingested = 3))

    // Short batchFlushMs so the worker finishes quickly on its own once
    // running=false; close() just waits for the natural exit.
    val s = newSink(batchFlushMs = 20L)
    (1 to 3).foreach(i => s.send(runEvent(s"r-$i")))

    s.close()

    val recorded = server.takeRequest(3, TimeUnit.SECONDS)
    recorded should not be null
    Lineage.IngestBatchRequest.parseFrom(recorded.getBody.readByteArray())
      .getEventsCount shouldBe 3
    s.sentCount shouldBe 3L
  }
}
