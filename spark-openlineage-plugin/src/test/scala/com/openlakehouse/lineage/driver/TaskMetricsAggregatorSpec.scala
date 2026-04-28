package com.openlakehouse.lineage.driver

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.openlakehouse.lineage.common.ExecutorTaskMetrics

final class TaskMetricsAggregatorSpec extends AnyFunSuite with Matchers with OptionValues {

  private def metrics(
      executionId: Option[Long],
      recordsWritten: Long = 0L,
      bytesWritten: Long = 0L,
      peakMem: Long = 0L,
      failed: Boolean = false
  ): ExecutorTaskMetrics = ExecutorTaskMetrics(
    executionId              = executionId,
    stageId                  = 0,
    stageAttemptNumber       = 0,
    taskAttemptId            = 1L,
    executorId               = "e",
    host                     = "h",
    recordsRead              = 0L,
    bytesRead                = 0L,
    recordsWritten           = recordsWritten,
    bytesWritten             = bytesWritten,
    executorRunTimeMs        = 10L,
    executorCpuTimeNs        = 1000L,
    jvmGcTimeMs              = 1L,
    peakExecutionMemoryBytes = peakMem,
    failed                   = failed,
    failureReason            = None
  )

  test("records sum per executionId and expose via snapshot") {
    val agg = new TaskMetricsAggregator
    agg.record(metrics(Some(1L), recordsWritten = 100L, bytesWritten = 1000L, peakMem = 500L))
    agg.record(metrics(Some(1L), recordsWritten =  50L, bytesWritten =  500L, peakMem = 700L))
    agg.record(metrics(Some(2L), recordsWritten =   7L, bytesWritten =   70L))

    val snap1 = agg.snapshot(1L).value
    snap1.taskCount shouldBe 2L
    snap1.recordsWritten shouldBe 150L
    snap1.bytesWritten shouldBe 1500L
    snap1.peakExecutionMemoryBytes shouldBe 700L // max, not sum

    val snap2 = agg.snapshot(2L).value
    snap2.taskCount shouldBe 1L

    // snapshot removes the entry.
    agg.snapshot(1L) shouldBe None
    agg.executionIds shouldBe Set.empty
  }

  test("failed tasks are counted separately") {
    val agg = new TaskMetricsAggregator
    agg.record(metrics(Some(10L), failed = false))
    agg.record(metrics(Some(10L), failed = true))
    agg.record(metrics(Some(10L), failed = true))

    val snap = agg.snapshot(10L).value
    snap.taskCount shouldBe 3L
    snap.failedTaskCount shouldBe 2L
  }

  test("metrics without executionId land in the orphaned bucket") {
    val agg = new TaskMetricsAggregator
    agg.record(metrics(None, recordsWritten = 5L))
    agg.record(metrics(None, recordsWritten = 3L))
    agg.orphanedSnapshot.recordsWritten shouldBe 8L
    agg.orphanedSnapshot.taskCount shouldBe 2L
  }

  test("asFacets yields a stable string map shape") {
    val agg = new TaskMetricsAggregator
    agg.record(metrics(Some(7L), recordsWritten = 42L))
    val facets = agg.snapshot(7L).value.asFacets
    facets("taskCount") shouldBe "1"
    facets("recordsWritten") shouldBe "42"
    facets.keySet should contain allOf ("bytesRead", "jvmGcTimeMs", "peakExecutionMemoryBytes")
  }

  test("record() is safe under concurrent producers") {
    val agg      = new TaskMetricsAggregator
    val executor = Executors.newFixedThreadPool(8)
    val latch    = new CountDownLatch(8)
    val perThread = 2000

    (1 to 8).foreach { _ =>
      executor.submit(new Runnable {
        def run(): Unit = {
          try {
            var i = 0
            while (i < perThread) {
              agg.record(metrics(Some(1L), recordsWritten = 1L))
              i += 1
            }
          } finally latch.countDown()
        }
      })
    }
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    executor.shutdown()

    val snap = agg.snapshot(1L).value
    snap.taskCount shouldBe (8L * perThread)
    snap.recordsWritten shouldBe (8L * perThread)
  }

}
