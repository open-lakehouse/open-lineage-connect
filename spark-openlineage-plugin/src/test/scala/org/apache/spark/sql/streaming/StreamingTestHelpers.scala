package org.apache.spark.sql.streaming

import java.util.{Map => JMap, UUID}

import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryTerminatedEvent}

/**
 * Test-only helpers that live in `org.apache.spark.sql.streaming` so they can
 * construct listener events whose ctors are `private[spark]`.
 *
 * Spark doesn't publish an official test-utils jar for 4.x that exposes these,
 * so we duck under the package-private wall rather than reach for reflection
 * every time.
 */
object StreamingTestHelpers {

  def newProgressEvent(progress: StreamingQueryProgress): QueryProgressEvent =
    new QueryProgressEvent(progress)

  def newTerminatedEvent(
      id: UUID,
      runId: UUID,
      exception: Option[String] = None,
      errorClassOnException: Option[String] = None
  ): QueryTerminatedEvent =
    new QueryTerminatedEvent(id, runId, exception, errorClassOnException)

  def newProgress(
      id: UUID,
      runId: UUID,
      name: String,
      timestamp: String,
      batchId: Long,
      sources: Array[SourceProgress],
      sink: SinkProgress
  ): StreamingQueryProgress = new StreamingQueryProgress(
    id,
    runId,
    name,
    timestamp,
    batchId,
    42L,
    java.util.Collections.emptyMap[String, java.lang.Long](),
    java.util.Collections.emptyMap[String, String](),
    Array.empty,
    sources,
    sink,
    java.util.Collections.emptyMap[String, org.apache.spark.sql.Row]()
  )

  def newSourceProgress(description: String): SourceProgress = new SourceProgress(
    description,
    "0",     // startOffset
    "1",     // endOffset
    "1",     // latestOffset
    100L,    // numInputRows
    10.0,    // inputRowsPerSecond
    9.0,     // processedRowsPerSecond
    java.util.Collections.emptyMap[String, String]()
  )

  def newSinkProgress(description: String): SinkProgress =
    new SinkProgress(description, 0L, java.util.Collections.emptyMap[String, String]())
}
