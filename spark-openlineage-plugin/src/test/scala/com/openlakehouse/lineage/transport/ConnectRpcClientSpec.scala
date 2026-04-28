package com.openlakehouse.lineage.transport

import java.util.concurrent.TimeUnit

import okhttp3.mockwebserver.{MockResponse, MockWebServer, RecordedRequest}
import okio.Buffer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import lineage.v1.Lineage

/**
 * Round-trips `IngestBatchRequest` / `IngestBatchResponse` over a live local
 * HTTP server (MockWebServer). This is the smallest test that still exercises:
 *
 *   - OkHttp wiring (timeouts, pooling, headers)
 *   - Connect unary framing (POST to service path, `application/proto`)
 *   - Proto serialization in both directions
 *   - Error mapping for non-200 responses
 */
final class ConnectRpcClientSpec extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  private var server: MockWebServer = _
  private def client: ConnectRpcClient = new ConnectRpcClient(
    baseUrl = server.url("/").toString.stripSuffix("/"),
    okHttp  = ConnectRpcClient.defaultOkHttp(connectTimeoutMs = 500L, readTimeoutMs = 1500L)
  )

  override def beforeEach(): Unit = {
    server = new MockWebServer()
    server.start()
  }

  override def afterEach(): Unit = {
    server.shutdown()
  }

  private def sampleRequest: Lineage.IngestBatchRequest =
    Lineage.IngestBatchRequest.newBuilder()
      .addEvents(Lineage.RunEvent.newBuilder()
        .setEventType("START")
        .setProducer("test")
        .setSchemaUrl("test://schema")
        .setRun(Lineage.Run.newBuilder().setRunId("r-1"))
        .setJob(Lineage.Job.newBuilder().setNamespace("ns").setName("j")))
      .build()

  private def successBody(ingested: Int): Buffer = {
    val resp = Lineage.IngestBatchResponse.newBuilder().setIngested(ingested).build()
    new Buffer().write(resp.toByteArray)
  }

  test("unary POSTs proto body to the service path with Connect headers") {
    server.enqueue(
      new MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", ConnectRpcClient.ProtoContentType)
        .setBody(successBody(ingested = 1))
    )

    val resp = client.unary(
      LineageServiceClient.IngestBatchPath,
      sampleRequest,
      Lineage.IngestBatchResponse.parser()
    )

    resp.isSuccess shouldBe true
    resp.get.getIngested shouldBe 1

    val recorded: RecordedRequest = server.takeRequest(2, TimeUnit.SECONDS)
    recorded should not be null
    recorded.getMethod shouldBe "POST"
    recorded.getPath shouldBe LineageServiceClient.IngestBatchPath
    recorded.getHeader("Content-Type") shouldBe ConnectRpcClient.ProtoContentType
    recorded.getHeader("Connect-Protocol-Version") shouldBe "1"

    // Verify the server received a valid IngestBatchRequest with our payload.
    val bodyBytes = recorded.getBody.readByteArray()
    val parsed    = Lineage.IngestBatchRequest.parseFrom(bodyBytes)
    parsed.getEventsCount shouldBe 1
    parsed.getEvents(0).getRun.getRunId shouldBe "r-1"
  }

  test("HTTP 503 is wrapped in a retriable ConnectRpcException") {
    server.enqueue(
      new MockResponse()
        .setResponseCode(503)
        .setBody("service unavailable")
    )

    val resp = client.unary(
      LineageServiceClient.IngestBatchPath,
      sampleRequest,
      Lineage.IngestBatchResponse.parser()
    )

    resp.isFailure shouldBe true
    val ex = resp.failed.get.asInstanceOf[ConnectRpcException]
    ex.httpStatus shouldBe 503
    ex.connectCode shouldBe "unavailable"
    ex.isRetriable shouldBe true
  }

  test("HTTP 400 is non-retriable (client error)") {
    server.enqueue(new MockResponse().setResponseCode(400).setBody("bad input"))
    val resp = client.unary(
      LineageServiceClient.IngestBatchPath, sampleRequest,
      Lineage.IngestBatchResponse.parser()
    )
    resp.isFailure shouldBe true
    resp.failed.get.asInstanceOf[ConnectRpcException].isRetriable shouldBe false
  }

  test("mapHttpToConnectCode covers the spec-defined statuses") {
    ConnectRpcClient.mapHttpToConnectCode(400) shouldBe "invalid_argument"
    ConnectRpcClient.mapHttpToConnectCode(401) shouldBe "unauthenticated"
    ConnectRpcClient.mapHttpToConnectCode(404) shouldBe "not_found"
    ConnectRpcClient.mapHttpToConnectCode(429) shouldBe "unavailable"
    ConnectRpcClient.mapHttpToConnectCode(500) shouldBe "internal"
    ConnectRpcClient.mapHttpToConnectCode(503) shouldBe "unavailable"
    ConnectRpcClient.mapHttpToConnectCode(418) shouldBe "unknown"
  }

  test("LineageServiceClient.ingestBatch short-circuits empty payloads") {
    // No enqueued MockResponse — if we accidentally hit the server, `takeRequest`
    // with a zero timeout would expose it.
    val svc = new LineageServiceClient(client)
    val resp = svc.ingestBatch(Seq.empty)
    resp.isSuccess shouldBe true
    resp.get.getIngested shouldBe 0
    server.getRequestCount shouldBe 0
  }
}
