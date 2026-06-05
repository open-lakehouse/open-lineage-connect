package com.openlakehouse.lineage.transport

/**
 * Exponential backoff with optional symmetric jitter.
 *
 * Kept as a pure function (no `Thread.sleep`, no clock) so the backoff schedule
 * can be unit-tested deterministically by supplying a fixed `rng`.
 */
object RetryBackoff {

  /**
   * Compute the delay before retry number `attempt` (1-based: the first retry
   * is `attempt = 1`).
   *
   * The exponential base is `baseMs * 2^(attempt - 1)`. With a `jitterFactor`
   * `f` in `[0, 1]` and `rng` drawing from `[0, 1)`:
   *
   * {{{
   *   result = base * (1 - f) + rng * (2 * f * base)
   * }}}
   *
   * which ranges over `[base * (1 - f), base * (1 + f))`. `f = 0` returns the
   * exact exponential value (no jitter); `rng` is not consulted in that case.
   * Inputs are clamped: `jitterFactor` to `[0, 1]`, `rng` output to `[0, 1]`,
   * and the result floored at `0`.
   */
  def compute(baseMs: Long, attempt: Int, jitterFactor: Double, rng: () => Double): Long = {
    require(attempt >= 1, s"attempt must be >= 1, got $attempt")
    val exp = baseMs.toDouble * math.pow(2.0, (attempt - 1).toDouble)
    val f   = clamp01(jitterFactor)
    if (f == 0.0) {
      math.max(0L, exp.toLong)
    } else {
      val r        = clamp01(rng())
      val jittered = exp * (1.0 - f) + r * (2.0 * f * exp)
      math.max(0L, jittered.toLong)
    }
  }

  private def clamp01(v: Double): Double = math.max(0.0, math.min(1.0, v))
}
