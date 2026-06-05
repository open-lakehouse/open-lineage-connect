package com.openlakehouse.lineage.transport

import scala.collection.mutable.ListBuffer

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class RateLimiterSpec extends AnyFunSuite with Matchers {

  private def msToNanos(ms: Long): Long = ms * 1000000L

  test("a non-positive interval disables rate limiting") {
    val slept = ListBuffer.empty[Long]
    val rl = new RateLimiter(0L, clockNanos = () => 0L, sleeper = slept += _)
    rl.acquire() shouldBe 0L
    rl.acquire() shouldBe 0L
    slept shouldBe empty
  }

  test("the first acquire never sleeps; the second waits out the remaining interval") {
    val slept = ListBuffer.empty[Long]
    // Clock readings, in order acquire() consumes them:
    //   acquire #1: get(last==MIN → no wait), set(t=0)
    //   acquire #2: elapsed read at t=30ms → wait 100-30=70ms, then set(t=...)
    val times = Iterator(
      msToNanos(0),   // #1 set
      msToNanos(30),  // #2 elapsed check
      msToNanos(100)  // #2 set
    )
    val rl = new RateLimiter(100L, clockNanos = () => times.next(), sleeper = slept += _)

    rl.acquire() shouldBe 0L
    rl.acquire() shouldBe 70L
    slept.toList shouldBe List(70L)
  }

  test("no sleep when more than the interval has already elapsed") {
    val slept = ListBuffer.empty[Long]
    val times = Iterator(
      msToNanos(0),    // #1 set
      msToNanos(500),  // #2 elapsed check (well past the 100ms interval)
      msToNanos(500)   // #2 set
    )
    val rl = new RateLimiter(100L, clockNanos = () => times.next(), sleeper = slept += _)

    rl.acquire() shouldBe 0L
    rl.acquire() shouldBe 0L
    slept shouldBe empty
  }
}
