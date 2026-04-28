package com.openlakehouse.lineage.transport

import scala.jdk.CollectionConverters._
import scala.util.Try

import lineage.v1.Lineage

/**
 * Typed wrapper over `ConnectRpcClient` that speaks the methods of the
 * `lineage.v1.LineageService` gRPC/Connect service defined in
 * `proto/lineage/v1/lineage.proto`.
 *
 * Every method is synchronous and returns a `Try` — asynchrony and batching
 * are the responsibility of `ConnectRpcEventSink`, not this layer. Keeping
 * the client synchronous makes failures trivially observable (one `Try` per
 * call) and keeps the call graph flat.
 */
final class LineageServiceClient(transport: ConnectRpcClient) {

  /**
   * `rpc IngestBatch(IngestBatchRequest) returns (IngestBatchResponse)`.
   *
   * Empty batches are treated as a local no-op (no HTTP call) because the
   * service is within its rights to reject empty batches and we'd rather
   * not burn a network round-trip on nothing.
   */
  def ingestBatch(events: Seq[Lineage.RunEvent]): Try[Lineage.IngestBatchResponse] = {
    if (events.isEmpty)
      return Try(Lineage.IngestBatchResponse.newBuilder().setIngested(0).build())

    val req = Lineage.IngestBatchRequest.newBuilder()
      .addAllEvents(events.asJava)
      .build()

    transport.unary(
      servicePath = LineageServiceClient.IngestBatchPath,
      request     = req,
      respParser  = Lineage.IngestBatchResponse.parser()
    )
  }

  /** `rpc IngestEvent(IngestEventRequest) returns (IngestEventResponse)`. */
  def ingestEvent(event: Lineage.RunEvent): Try[Lineage.IngestEventResponse] = {
    val req = Lineage.IngestEventRequest.newBuilder().setEvent(event).build()
    transport.unary(
      servicePath = LineageServiceClient.IngestEventPath,
      request     = req,
      respParser  = Lineage.IngestEventResponse.parser()
    )
  }
}

object LineageServiceClient {
  val ServiceName: String      = "lineage.v1.LineageService"
  val IngestBatchPath: String  = s"/$ServiceName/IngestBatch"
  val IngestEventPath: String  = s"/$ServiceName/IngestEvent"
}
