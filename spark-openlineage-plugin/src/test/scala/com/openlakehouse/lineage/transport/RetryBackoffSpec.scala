package com.openlakehouse.lineage.transport

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class RetryBackoffSpec extends AnyFunSuite with Matchers {

  // rng is never consulted when jitterFactor == 0, so a throwing rng proves it.
  private val throwingRng: () => Double = () => fail("rng must not be consulted when jitter is disabled")

  test("no jitter yields a pure exponential schedule") {
    RetryBackoff.compute(200L, attempt = 1, jitterFactor = 0.0, rng = throwingRng) shouldBe 200L
    RetryBackoff.compute(200L, attempt = 2, jitterFactor = 0.0, rng = throwingRng) shouldBe 400L
    RetryBackoff.compute(200L, attempt = 3, jitterFactor = 0.0, rng = throwingRng) shouldBe 800L
  }

  test("symmetric jitter maps rng endpoints to the band edges") {
    // base for attempt 1 == 200. f = 0.5 → band [100, 300].
    RetryBackoff.compute(200L, attempt = 1, jitterFactor = 0.5, rng = () => 0.0)  shouldBe 100L
    RetryBackoff.compute(200L, attempt = 1, jitterFactor = 0.5, rng = () => 0.5)  shouldBe 200L
    // rng → 1.0 hits the open upper edge: 200 * (1.5) = 300.
    RetryBackoff.compute(200L, attempt = 1, jitterFactor = 0.5, rng = () => 1.0)  shouldBe 300L
  }

  test("jitter scales with the exponential base across attempts") {
    // attempt 3 → base 800; f = 0.25 → band [600, 1000]; rng 0.5 → 800.
    RetryBackoff.compute(200L, attempt = 3, jitterFactor = 0.25, rng = () => 0.5) shouldBe 800L
    RetryBackoff.compute(200L, attempt = 3, jitterFactor = 0.25, rng = () => 0.0) shouldBe 600L
  }

  test("jitterFactor and rng are clamped to [0,1]") {
    // jitterFactor > 1 clamps to 1 → band [0, 2*base); rng < 0 clamps to 0.
    RetryBackoff.compute(200L, attempt = 1, jitterFactor = 5.0, rng = () => -1.0) shouldBe 0L
    // rng > 1 clamps to 1 with f = 1 → 2 * base.
    RetryBackoff.compute(200L, attempt = 1, jitterFactor = 1.0, rng = () => 2.0)  shouldBe 400L
  }

  test("attempt below 1 is rejected") {
    an[IllegalArgumentException] should be thrownBy
      RetryBackoff.compute(200L, attempt = 0, jitterFactor = 0.0, rng = throwingRng)
  }
}
