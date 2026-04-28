package com.openlakehouse.lineage.driver

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import lineage.v1.Lineage

/**
 * Abstract destination for built `RunEvent`s.
 *
 * Implementations MUST be thread-safe (the listener may be invoked from Spark's
 * SparkListenerBus thread, the async query execution thread, and user-code
 * threads concurrently) and MUST NOT block — the downstream ConnectRPC emitter
 * is expected to handle batching, backpressure, and retries internally via a
 * bounded queue.
 *
 * `close()` should drain/flush any in-flight work. Implementations may time out
 * on close; if they do, they should log and return rather than hanging the
 * Spark driver's shutdown path.
 */
trait EventSink extends AutoCloseable {
  def send(event: Lineage.RunEvent): Unit
  override def close(): Unit = ()
}

object EventSink {

  /** Drops every event on the floor. Useful when the plugin is "loaded but inert." */
  object Noop extends EventSink {
    def send(event: Lineage.RunEvent): Unit = ()
  }

  /**
   * Thread-safe in-memory sink for tests. Preserves insertion order.
   */
  final class InMemory extends EventSink {
    private val buffer = new ConcurrentLinkedQueue[Lineage.RunEvent]()

    def send(event: Lineage.RunEvent): Unit = {
      buffer.add(event); ()
    }

    /** Snapshot of events captured so far (in insertion order). */
    def events: Seq[Lineage.RunEvent] = buffer.iterator().asScala.toList

    def size: Int = buffer.size()

    def clear(): Unit = buffer.clear()

    override def close(): Unit = ()
  }

  /**
   * Fan-out sink: each event is forwarded to every downstream sink in order.
   * A failure in one downstream does not prevent delivery to the others.
   */
  final class Broadcast(sinks: EventSink*) extends EventSink {
    def send(event: Lineage.RunEvent): Unit =
      sinks.foreach { s =>
        try s.send(event)
        catch { case _: Throwable => /* swallow; each sink owns its own error policy */ () }
      }
    override def close(): Unit = sinks.foreach { s =>
      try s.close() catch { case _: Throwable => () }
    }
  }
}
