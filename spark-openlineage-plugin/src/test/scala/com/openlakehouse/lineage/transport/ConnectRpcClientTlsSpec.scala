package com.openlakehouse.lineage.transport

import java.io.FileOutputStream
import java.nio.file.Files
import java.security.KeyStore

import scala.util.{Failure, Success}

import okhttp3.mockwebserver.{MockResponse, MockWebServer}
import okhttp3.tls.{HandshakeCertificates, HeldCertificate}
import okio.Buffer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.openlakehouse.lineage.TlsConfig

import lineage.v1.Lineage

/**
 * Exercises the TLS transport path end-to-end against an HTTPS `MockWebServer`
 * fronted by a self-signed certificate. The client only succeeds when our
 * `okHttpForTls` wiring loads the trust store containing that certificate —
 * which is the whole point of the secure transport.
 */
final class ConnectRpcClientTlsSpec extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  private var server: MockWebServer = _
  private var serverCert: HeldCertificate = _

  override def beforeEach(): Unit = {
    serverCert = new HeldCertificate.Builder()
      .commonName("localhost")
      .addSubjectAlternativeName("localhost")
      .build()
    val serverCerts = new HandshakeCertificates.Builder()
      .heldCertificate(serverCert)
      .build()
    server = new MockWebServer()
    server.useHttps(serverCerts.sslSocketFactory(), false)
    server.start()
  }

  override def afterEach(): Unit = {
    server.shutdown()
  }

  private def successResponse(): MockResponse = {
    val resp = Lineage.IngestBatchResponse.newBuilder().setIngested(1).build()
    new MockResponse()
      .setResponseCode(200)
      .addHeader("Content-Type", ConnectRpcClient.ProtoContentType)
      .setBody(new Buffer().write(resp.toByteArray))
  }

  private def runEvent(): Lineage.RunEvent =
    Lineage.RunEvent.newBuilder()
      .setEventType("START")
      .setRun(Lineage.Run.newBuilder().setRunId("tls-run"))
      .setJob(Lineage.Job.newBuilder().setNamespace("ns").setName("j"))
      .build()

  /** Persist the server's self-signed cert into a PKCS12 trust store on disk. */
  private def writeTrustStore(password: String): String = {
    val ks = KeyStore.getInstance("PKCS12")
    ks.load(null, null)
    ks.setCertificateEntry("server", serverCert.certificate)
    val file = Files.createTempFile("ol-truststore", ".p12").toFile
    file.deleteOnExit()
    val out = new FileOutputStream(file)
    try ks.store(out, password.toCharArray)
    finally out.close()
    file.getAbsolutePath
  }

  test("a client trusting the server cert completes the TLS handshake and call") {
    server.enqueue(successResponse())
    val password = "changeit"
    val tls = TlsConfig(
      trustStorePath     = Some(writeTrustStore(password)),
      trustStorePassword = Some(password),
      trustStoreType     = "PKCS12"
    )
    val client = new ConnectRpcClient(
      baseUrl = server.url("/").toString.stripSuffix("/"),
      okHttp  = ConnectRpcClient.okHttpForTls(tls, connectTimeoutMs = 1000L, readTimeoutMs = 2000L)
    )

    new LineageServiceClient(client).ingestBatch(Seq(runEvent())) match {
      case Success(resp) => resp.getIngested shouldBe 1
      case Failure(t)    => fail(s"expected the TLS call to succeed, got: $t")
    }
  }

  test("a client without the server cert in its trust store fails the handshake") {
    server.enqueue(successResponse())
    // Point the trust store at an EMPTY PKCS12 (no certificates) so the
    // self-signed server cert is genuinely untrusted.
    val password = "changeit"
    val emptyStorePath = {
      val ks = KeyStore.getInstance("PKCS12")
      ks.load(null, null)
      val file = Files.createTempFile("ol-empty-truststore", ".p12").toFile
      file.deleteOnExit()
      val out = new FileOutputStream(file)
      try ks.store(out, password.toCharArray)
      finally out.close()
      file.getAbsolutePath
    }
    val tls = TlsConfig(
      trustStorePath     = Some(emptyStorePath),
      trustStorePassword = Some(password),
      trustStoreType     = "PKCS12"
    )
    val client = new ConnectRpcClient(
      baseUrl = server.url("/").toString.stripSuffix("/"),
      okHttp  = ConnectRpcClient.okHttpForTls(tls, connectTimeoutMs = 1000L, readTimeoutMs = 2000L)
    )

    new LineageServiceClient(client).ingestBatch(Seq(runEvent())) match {
      case Success(_) => fail("expected the handshake to fail against an untrusted self-signed cert")
      case Failure(_) => succeed
    }
  }
}
