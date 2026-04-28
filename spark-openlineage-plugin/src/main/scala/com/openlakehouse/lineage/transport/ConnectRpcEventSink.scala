package com.openlakehouse.lineage.transport

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.slf4j.{Logger, LoggerFactory}

import com.openlakehouse.lineage.driver.EventSink

import lineage.v1.Lineage

/**
 * `EventSink` that buffers `RunEvent`s in a bounded queue and flushes them
 * to a `LineageServiceClient` from a single background worker thread.
 *
 * ### Design invariants
 *
 *   - `send()` is non-blocking. If the queue is full we drop the event and
 *     increment a counter. A dropped event is strictly preferable to a blocked
 *     Spark driver.
 *   - One worker thread per sink. Multi-threaded flushing buys us nothing:
 *     the service is a single downstream bottleneck, and the driver already
 *     serializes events into the queue fast enough to saturate any reasonable
 *     batch size.
 *   - Worker drains batches of up to `batchMaxEvents` items or waits at most
 *     `batchFlushMs` for the next item before sending whatever it has.
 *   - `close()` signals the worker to flush within a deadline and joins it.
 *     We do not guarantee zero data loss on close; we guarantee bounded
 *     latency on shutdown.
 *
 * ### Retry policy
 *
 * Retries are intentionally modest (a couple of attempts with short fixed
 * backoff). The lineage service is downstream-of-record and we expect it to
 * be mostly available; persistent outages should be surfaced via the Spark
 * driver's logs, not papered over with aggressive retries that pressure the
 * queue.
 */
final class ConnectRpcEventSink(
    client: LineageServiceClient,
    queueCapacity: Int      = 1024,
    batchMaxEvents: Int     = 128,
    batchFlushMs: Long      = 250L,
    shutdownDrainMs: Long   = 5000L,
    maxRetries: Int         = 2,
    retryBackoffMs: Long    = 200L,
    threadNamePrefix: String = "openlineage-emitter"
) extends EventSink {

  require(queueCapacity > 0, "queueCapacity must be > 0")
  require(batchMaxEvents > 0, "batchMaxEvents must be > 0")

  // Use SLF4J directly rather than Spark's `Logging` trait. The trait eagerly
  // initializes Log4j2 via `LogManager.<clinit>` on first log call, which
  // races and fails under fork-tests where the background worker thread has a
  // minimal context classloader. SLF4J has no such init path.
  private val logger: Logger = LoggerFactory.getLogger(classOf[ConnectRpcEventSink])

  private val queue   = new ArrayBlockingQueue[Lineage.RunEvent](queueCapacity)
  private val running = new AtomicBoolean(true)
  private val dropped = new AtomicLong(0L)
  private val sent    = new AtomicLong(0L)
  private val failed  = new AtomicLong(0L)

  private val worker: Thread = {
    val t = new Thread(new WorkerRunnable(), s"$threadNamePrefix-${System.identityHashCode(this).toHexString}")
    t.setDaemon(true)
    // Spark's `Logging` trait initializes Log4j2's LogManager lazily on first
    // use. On a freshly-created thread (particularly under `sbt` fork tests)
    // the context classloader can be `null`, which makes Log4j's ServiceLoader
    // lookup throw an NPE. Pin the classloader to the one that loaded us so
    // `logDebug`/`logWarning` calls from the worker thread are safe.
    t.setContextClassLoader(classOf[ConnectRpcEventSink].getClassLoader)
    t
  }
  worker.start()

  override def send(event: Lineage.RunEvent): Unit = {
    if (!running.get()) return
    if (!queue.offer(event)) {
      val total = dropped.incrementAndGet()
      if ((total & (total - 1)) == 0) {
        // Log at power-of-two drop counts to avoid filling logs under sustained backpressure.
        logger.warn("OpenLineage event queue is full; dropped {} events so far", total)
      }
    }
  }

  override def close(): Unit = {
    if (!running.compareAndSet(true, false)) return

    // Let the worker exit on its own: `running=false` plus its bounded
    // `queue.poll(batchFlushMs)` means it drops out of the loop within
    // `batchFlushMs` of the queue going empty, *after* completing any
    // in-flight HTTP request. We deliberately avoid a pre-emptive
    // `Thread.interrupt()` here — interrupting OkHttp mid-call aborts the
    // request and loses events that were already about to be flushed.
    try worker.join(shutdownDrainMs)
    catch { case _: InterruptedException => Thread.currentThread().interrupt() }

    // Deadline backstop: if the worker is still stuck (e.g. hung HTTP call),
    // force it out. Any in-flight request is sacrificed at this point.
    if (worker.isAlive) {
      logger.warn("OpenLineage emitter worker did not exit within {} ms; interrupting", shutdownDrainMs)
      worker.interrupt()
      try worker.join(1000L)
      catch { case _: InterruptedException => Thread.currentThread().interrupt() }
    }

    // Anything still in the queue (worker was interrupted, or edge-case where
    // the queue received events between the last poll and `running=false`)
    // is flushed synchronously from the caller's thread.
    val pending = new java.util.ArrayList[Lineage.RunEvent]()
    queue.drainTo(pending)
    if (!pending.isEmpty) {
      import scala.jdk.CollectionConverters._
      flushBatch(pending.asScala.toList)
    }

    logger.info(
      "OpenLineage ConnectRpcEventSink closed (sent={}, failed={}, dropped={})",
      sent.get(), failed.get(), dropped.get()
    )
  }

  /** Test hooks — not part of the public `EventSink` contract. */
  def droppedCount: Long = dropped.get()
  def sentCount: Long    = sent.get()
  def failedCount: Long  = failed.get()
  def queueDepth: Int    = queue.size()

  private final class WorkerRunnable extends Runnable {
    def run(): Unit = {
      val batch = new ArrayBuffer[Lineage.RunEvent](batchMaxEvents)

      while (running.get() || !queue.isEmpty) {
        batch.clear()
        try {
          // First item: block up to flushMs. This is the deadline for building
          // a partial batch when the producer is idle.
          val first = queue.poll(batchFlushMs, TimeUnit.MILLISECONDS)
          if (first != null) {
            batch += first
            var remaining = batchMaxEvents - 1
            while (remaining > 0 && !queue.isEmpty) {
              val next = queue.poll()
              if (next == null) remaining = 0
              else {
                batch += next
                remaining -= 1
              }
            }
            flushBatch(batch.toList)
          }
        } catch {
          case _: InterruptedException =>
            // Only break out of the inner poll; the outer loop will re-check
            // `running` and drain any remaining items before terminating.
            ()
          case NonFatal(t) =>
            logger.warn("OpenLineage emitter worker encountered an unexpected error", t)
        }
      }
    }
  }

  private def flushBatch(batch: Seq[Lineage.RunEvent]): Unit = {
    if (batch.isEmpty) return
    var attempt   = 0
    var lastError: Throwable = null

    while (attempt <= maxRetries) {
      client.ingestBatch(batch) match {
        case Success(resp) =>
          sent.addAndGet(batch.size.toLong)
          if (logger.isDebugEnabled) {
            logger.debug(
              "OpenLineage ingested {} events (server reported ingested={})",
              batch.size, resp.getIngested
            )
          }
          return
        case Failure(t: ConnectRpcException) if t.isRetriable && attempt < maxRetries =>
          lastError = t
          attempt += 1
          sleepQuietly(retryBackoffMs * (1L << (attempt - 1)))
        case Failure(t) =>
          failed.addAndGet(batch.size.toLong)
          logger.warn(
            "OpenLineage ingest failed (batch={}, attempt={}): {}",
            batch.size, attempt, t.getMessage
          )
          return
      }
    }

    if (lastError != null) {
      failed.addAndGet(batch.size.toLong)
      logger.warn(
        "OpenLineage ingest failed after {} retries (batch={}): {}",
        maxRetries, batch.size, lastError.getMessage
      )
    }
  }

  private def sleepQuietly(ms: Long): Unit =
    try Thread.sleep(ms)
    catch { case _: InterruptedException => Thread.currentThread().interrupt() }
}
