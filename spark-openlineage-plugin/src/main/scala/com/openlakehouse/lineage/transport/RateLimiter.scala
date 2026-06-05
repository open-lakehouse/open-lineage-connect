package com.openlakehouse.lineage.transport

import java.util.concurrent.atomic.AtomicLong

/**
 * Minimal min-interval rate limiter for the lineage endpoint.
 *
 * `acquire()` blocks (via the injected `sleeper`) until at least
 * `minIntervalMs` has elapsed since the previous `acquire()`. A non-positive
 * `minIntervalMs` disables the limiter entirely (every `acquire()` is a no-op).
 *
 * The clock is monotonic nanos. The sink only calls this from its single
 * worker thread, so strict cross-thread fairness is unnecessary; the
 * `AtomicLong` is used solely to publish the timestamp safely.
 *
 * The `clockNanos` and `sleeper` seams exist so the spacing logic can be
 * unit-tested without real time passing.
 */
final class RateLimiter(
    minIntervalMs: Long,
    clockNanos: () => Long = () => System.nanoTime(),
    sleeper: Long => Unit = ms => Thread.sleep(ms)
) {

  private val lastNanos = new AtomicLong(Long.MinValue)

  /** Returns the number of milliseconds this call slept (0 if none). */
  def acquire(): Long = {
    if (minIntervalMs <= 0L) return 0L
    val last = lastNanos.get()
    var slept = 0L
    if (last != Long.MinValue) {
      val elapsedMs = (clockNanos() - last) / 1000000L
      val waitMs    = minIntervalMs - elapsedMs
      if (waitMs > 0L) {
        sleeper(waitMs)
        slept = waitMs
      }
    }
    lastNanos.set(clockNanos())
    slept
  }
}
