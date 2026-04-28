package com.openlakehouse.lineage.driver

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import scala.jdk.CollectionConverters._

import com.openlakehouse.lineage.common.ExecutorTaskMetrics

/**
 * Driver-side aggregator for `ExecutorTaskMetrics` messages arriving via
 * `LineageDriverPlugin.receive`. Keyed by SQL execution id.
 *
 * The aggregator is deliberately tiny: just per-executionId counters, not a
 * list of every task payload. A Spark job with 10 000 tasks would otherwise
 * hold 10 000 case-class instances on the driver heap for the lifetime of
 * the query — we can do without that.
 *
 * Concurrency model:
 *   - `record()` is called from Spark's RPC dispatcher thread (driver side).
 *   - `snapshot()` / `clear()` are called from the QueryExecutionListener
 *     callback (execution thread).
 *   - All state lives in a `ConcurrentHashMap` of atomic counters; no locks.
 */
final class TaskMetricsAggregator {

  import TaskMetricsAggregator._

  private val perExecution = new ConcurrentHashMap[Long, Counters]()
  private val orphaned = new Counters  // task metrics with no executionId

  /** Fold a single task payload into the per-execution counters. */
  def record(m: ExecutorTaskMetrics): Unit = {
    val target = m.executionId match {
      case Some(id) => perExecution.computeIfAbsent(id, _ => new Counters)
      case None     => orphaned
    }
    target.merge(m)
  }

  /** Snapshot counters for one execution and remove them from the map. */
  def snapshot(executionId: Long): Option[Snapshot] =
    Option(perExecution.remove(executionId)).map(_.toSnapshot)

  /** For tests / diagnostics. */
  def orphanedSnapshot: Snapshot = orphaned.toSnapshot
  def size: Int                  = perExecution.size()

  /** Clear all state. Used on plugin shutdown. */
  def clear(): Unit = {
    perExecution.clear()
    orphaned.reset()
  }

  /** Diagnostic view for tests. */
  def executionIds: Set[Long] = perExecution.keys().asScala.toSet
}

object TaskMetricsAggregator {

  /**
   * Immutable per-execution aggregate, suitable for attaching to a RunContext
   * as facets. All counters are sums over the tasks observed so far for a
   * given executionId.
   */
  final case class Snapshot(
      taskCount: Long,
      failedTaskCount: Long,
      recordsRead: Long,
      bytesRead: Long,
      recordsWritten: Long,
      bytesWritten: Long,
      executorRunTimeMs: Long,
      executorCpuTimeNs: Long,
      jvmGcTimeMs: Long,
      peakExecutionMemoryBytes: Long
  ) {
    /** Represent as a facet map ready for merging into `RunContext.facets`. */
    def asFacets: Map[String, String] = Map(
      "taskCount"                -> taskCount.toString,
      "failedTaskCount"          -> failedTaskCount.toString,
      "recordsRead"              -> recordsRead.toString,
      "bytesRead"                -> bytesRead.toString,
      "recordsWritten"           -> recordsWritten.toString,
      "bytesWritten"             -> bytesWritten.toString,
      "executorRunTimeMs"        -> executorRunTimeMs.toString,
      "executorCpuTimeNs"        -> executorCpuTimeNs.toString,
      "jvmGcTimeMs"              -> jvmGcTimeMs.toString,
      "peakExecutionMemoryBytes" -> peakExecutionMemoryBytes.toString
    )
  }

  private final class Counters {
    val tasks    = new LongAdder
    val failed   = new LongAdder
    val recRead  = new LongAdder
    val byRead   = new LongAdder
    val recWr    = new LongAdder
    val byWr     = new LongAdder
    val runMs    = new LongAdder
    val cpuNs    = new LongAdder
    val gcMs     = new LongAdder
    val peakMem  = new AtomicLong(0L)

    def merge(m: ExecutorTaskMetrics): Unit = {
      tasks.increment()
      if (m.failed) failed.increment()
      recRead.add(m.recordsRead)
      byRead.add(m.bytesRead)
      recWr.add(m.recordsWritten)
      byWr.add(m.bytesWritten)
      runMs.add(m.executorRunTimeMs)
      cpuNs.add(m.executorCpuTimeNs)
      gcMs.add(m.jvmGcTimeMs)
      // Peak memory is a max, not a sum.
      updateMax(peakMem, m.peakExecutionMemoryBytes)
    }

    def toSnapshot: Snapshot = Snapshot(
      taskCount                = tasks.sum(),
      failedTaskCount          = failed.sum(),
      recordsRead              = recRead.sum(),
      bytesRead                = byRead.sum(),
      recordsWritten           = recWr.sum(),
      bytesWritten             = byWr.sum(),
      executorRunTimeMs        = runMs.sum(),
      executorCpuTimeNs        = cpuNs.sum(),
      jvmGcTimeMs              = gcMs.sum(),
      peakExecutionMemoryBytes = peakMem.get()
    )

    def reset(): Unit = {
      tasks.reset(); failed.reset()
      recRead.reset(); byRead.reset(); recWr.reset(); byWr.reset()
      runMs.reset(); cpuNs.reset(); gcMs.reset()
      peakMem.set(0L)
    }

    private def updateMax(target: AtomicLong, candidate: Long): Unit = {
      var current = target.get()
      while (candidate > current && !target.compareAndSet(current, candidate)) {
        current = target.get()
      }
    }
  }
}
