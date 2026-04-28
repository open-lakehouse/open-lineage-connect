package com.openlakehouse.lineage.driver

import java.time.Instant
import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import lineage.v1.Lineage
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class EventSinkSpec extends AnyFunSuite with Matchers {

  private def dummyEvent(tag: String): Lineage.RunEvent =
    new RunEventBuilder().build(RunContext(
      runId        = UUID.randomUUID(),
      job          = JobRef("ns", tag),
      eventTime    = Instant.EPOCH,
      status       = RunStatus.Start,
      inputs       = Seq.empty,
      outputs      = Seq.empty,
      executionId  = None,
      errorMessage = None,
      errorClass   = None,
      facets       = Map.empty
    ))

  test("Noop sink accepts events and returns without error") {
    noException should be thrownBy EventSink.Noop.send(dummyEvent("x"))
    noException should be thrownBy EventSink.Noop.close()
  }

  test("InMemory sink records events in insertion order") {
    val sink = new EventSink.InMemory
    sink.send(dummyEvent("a"))
    sink.send(dummyEvent("b"))
    sink.send(dummyEvent("c"))
    sink.size shouldBe 3
    sink.events.map(_.getJob.getName) shouldBe Seq("a", "b", "c")
  }

  test("InMemory sink is safe under concurrent producers") {
    val sink  = new EventSink.InMemory
    val pool  = Executors.newFixedThreadPool(4)
    val ready = new CountDownLatch(4)
    val go    = new CountDownLatch(1)

    (0 until 4).foreach { _ =>
      pool.submit(new Runnable {
        def run(): Unit = {
          ready.countDown()
          go.await()
          (0 until 100).foreach(_ => sink.send(dummyEvent("concurrent")))
        }
      })
    }
    ready.await()
    go.countDown()
    pool.shutdown()
    pool.awaitTermination(5, TimeUnit.SECONDS) shouldBe true

    sink.size shouldBe 400
  }

  test("Broadcast fans out to all sinks and survives a failing one") {
    val good1 = new EventSink.InMemory
    val good2 = new EventSink.InMemory
    val bad   = new EventSink {
      def send(event: Lineage.RunEvent): Unit = throw new RuntimeException("boom")
    }

    val bcast = new EventSink.Broadcast(good1, bad, good2)
    bcast.send(dummyEvent("e"))

    good1.size shouldBe 1
    good2.size shouldBe 1
  }
}
