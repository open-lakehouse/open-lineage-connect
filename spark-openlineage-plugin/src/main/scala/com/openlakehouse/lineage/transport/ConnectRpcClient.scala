package com.openlakehouse.lineage.transport

import java.io.IOException
import java.util.concurrent.TimeUnit

import scala.util.{Failure, Success, Try}

import com.google.protobuf.{Message, Parser}
import okhttp3.{Headers, MediaType, OkHttpClient, Request, RequestBody, Response}

/**
 * Minimal ConnectRPC unary-over-HTTP client backed by OkHttp.
 *
 * ## Why not `com.connectrpc.ProtocolClient`?
 *
 * connect-kotlin is the only ConnectRPC library that currently publishes to
 * Maven Central, but its client API is Kotlin-idiomatic: every generated method
 * is a `suspend fun`, callable only from a Kotlin coroutine scope (or via
 * `runBlocking`/callback bridges that are awkward from Scala). Rather than
 * plumb a coroutine dispatcher through our plugin, we speak the Connect wire
 * protocol directly — it's very simple for unary calls:
 *
 *   - POST `${baseUrl}/{service}/{method}`
 *   - `Content-Type: application/proto` (binary) or `application/json` (JSON)
 *   - Request body: the request message, serialized.
 *   - Success: HTTP 200, body is the response message, serialized.
 *   - Failure: non-200 HTTP status, body is a Connect error JSON document.
 *
 * This keeps the transport thin, synchronous, and easy to unit-test with
 * OkHttp's `MockWebServer` (no coroutines involved).
 *
 * Reference: https://connectrpc.com/docs/protocol#unary-request
 */
final class ConnectRpcClient(
    baseUrl: String,
    okHttp: OkHttpClient,
    extraHeaders: Map[String, String] = Map.empty
) {

  require(baseUrl != null && baseUrl.nonEmpty, "baseUrl must be non-empty")

  private val normalizedBase: String = baseUrl.stripSuffix("/")

  /**
   * Execute a Connect unary call.
   *
   * @param servicePath fully-qualified service path, e.g. `/lineage.v1.LineageService/IngestBatch`.
   * @param request     request message (any generated proto `Message`).
   * @param respParser  parser for the response message type (`Resp.parser()`).
   */
  def unary[Req <: Message, Resp <: Message](
      servicePath: String,
      request: Req,
      respParser: Parser[Resp]
  ): Try[Resp] = {
    val url  = normalizedBase + ensureLeadingSlash(servicePath)
    val body = RequestBody.create(request.toByteArray, ConnectRpcClient.ProtoMediaType)

    val headerBuilder = new Headers.Builder()
      .add("Content-Type", ConnectRpcClient.ProtoContentType)
      .add("Accept", ConnectRpcClient.ProtoContentType)
      .add("Connect-Protocol-Version", "1")
    extraHeaders.foreach { case (k, v) => headerBuilder.add(k, v) }

    val httpReq = new Request.Builder()
      .url(url)
      .headers(headerBuilder.build())
      .post(body)
      .build()

    Try(okHttp.newCall(httpReq).execute()).flatMap { response =>
      try parseResponse(response, respParser)
      finally response.close()
    }
  }

  private def parseResponse[Resp <: Message](
      response: Response,
      parser: Parser[Resp]
  ): Try[Resp] = {
    val code = response.code()
    val bytes = Option(response.body()).map(_.bytes()).getOrElse(Array.emptyByteArray)

    if (code == 200) {
      Try(parser.parseFrom(bytes))
    } else {
      val message = new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
      Failure(new ConnectRpcException(
        httpStatus = code,
        connectCode = ConnectRpcClient.mapHttpToConnectCode(code),
        message = s"Connect call failed (HTTP $code): ${message.take(2048)}"
      ))
    }
  }

  private def ensureLeadingSlash(s: String): String =
    if (s.startsWith("/")) s else "/" + s
}

object ConnectRpcClient {

  val ProtoContentType: String = "application/proto"
  val ProtoMediaType: MediaType = MediaType.get(ProtoContentType)

  /** HTTP status → Connect error code, per the Connect spec. */
  def mapHttpToConnectCode(status: Int): String = status match {
    case 400 => "invalid_argument"
    case 401 => "unauthenticated"
    case 403 => "permission_denied"
    case 404 => "not_found"
    case 408 => "deadline_exceeded"
    case 409 => "aborted"
    case 412 => "failed_precondition"
    case 413 => "resource_exhausted"
    case 415 => "internal"
    case 429 => "unavailable"
    case 431 => "resource_exhausted"
    case 502 => "unavailable"
    case 503 => "unavailable"
    case 504 => "unavailable"
    case _ if status >= 500 => "internal"
    case _                  => "unknown"
  }

  /**
   * Opinionated default OkHttp client. Short-ish timeouts because we would
   * rather drop an event than block the Spark driver on a hung lineage service.
   */
  def defaultOkHttp(connectTimeoutMs: Long = 2000L, readTimeoutMs: Long = 5000L): OkHttpClient =
    new OkHttpClient.Builder()
      .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
      .readTimeout(readTimeoutMs, TimeUnit.MILLISECONDS)
      .writeTimeout(readTimeoutMs, TimeUnit.MILLISECONDS)
      .retryOnConnectionFailure(true)
      .build()
}

/**
 * Thrown for any non-200 Connect response. Carries both the raw HTTP status
 * and the Connect-mapped code so callers can decide whether to retry.
 */
final class ConnectRpcException(
    val httpStatus: Int,
    val connectCode: String,
    message: String,
    cause: Throwable = null
) extends IOException(message, cause) {

  def isRetriable: Boolean = connectCode match {
    case "unavailable" | "deadline_exceeded" | "aborted" | "resource_exhausted" => true
    case "internal" | "unknown" if httpStatus >= 500                             => true
    case _                                                                      => false
  }
}
