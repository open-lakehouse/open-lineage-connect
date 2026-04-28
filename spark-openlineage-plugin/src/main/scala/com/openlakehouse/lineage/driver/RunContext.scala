package com.openlakehouse.lineage.driver

import java.time.Instant
import java.util.UUID

/**
 * Per-query state tracked on the driver. One `RunContext` corresponds to one
 * "observation" of a Spark query execution, regardless of whether it ultimately
 * succeeded, failed, or was abandoned.
 *
 * Design notes:
 *   - Immutable: lifecycle transitions return a new instance via `.copy(...)`.
 *   - `runId` is a UUID v4 generated on construction, independent of Spark's
 *     internal execution ID. We *also* carry `executionId` as a facet so users
 *     can cross-reference with the Spark UI, but OpenLineage identity is the
 *     UUID.
 *   - `inputs`/`outputs` are seed values captured at START. COMPLETE events
 *     may refine these if the plan reveals more information post-execution.
 */
final case class RunContext(
    runId: UUID,
    job: JobRef,
    eventTime: Instant,
    status: RunStatus,
    inputs: Seq[DatasetRef],
    outputs: Seq[DatasetRef],
    executionId: Option[Long],
    errorMessage: Option[String],
    errorClass: Option[String],
    facets: Map[String, String]
) {

  def withStatus(newStatus: RunStatus, now: Instant = Instant.now()): RunContext =
    copy(status = newStatus, eventTime = now)

  def withError(err: Throwable, now: Instant = Instant.now()): RunContext =
    copy(
      status       = RunStatus.Fail,
      eventTime    = now,
      errorClass   = Some(err.getClass.getName),
      errorMessage = Option(err.getMessage)
    )

  def withInputs(refs: Seq[DatasetRef]): RunContext =
    copy(inputs = DatasetRef.distinctByIdentity(refs))

  def withOutputs(refs: Seq[DatasetRef]): RunContext =
    copy(outputs = DatasetRef.distinctByIdentity(refs))

  def withFacet(key: String, value: String): RunContext =
    copy(facets = facets + (key -> value))

  /**
   * Merge a bundle of facets (e.g. a `TaskMetricsAggregator.Snapshot`) into
   * this context. Existing keys are preserved — the first write wins — so
   * late-arriving additions can't clobber data we deliberately set earlier.
   */
  def withFacets(extra: Map[String, String]): RunContext = {
    if (extra.isEmpty) this
    else copy(facets = extra ++ facets)
  }
}

object RunContext {

  /** Construct a fresh context in START state. */
  def start(
      job: JobRef,
      now: Instant = Instant.now(),
      executionId: Option[Long] = None,
      facets: Map[String, String] = Map.empty
  ): RunContext = RunContext(
    runId        = UUID.randomUUID(),
    job          = job,
    eventTime    = now,
    status       = RunStatus.Start,
    inputs       = Seq.empty,
    outputs      = Seq.empty,
    executionId  = executionId,
    errorMessage = None,
    errorClass   = None,
    facets       = facets
  )
}

/**
 * Identity of a Spark job, independent of run instance. Stable across retries
 * of the "same" query — this is what downstream graphs use to stitch runs
 * into lineage edges.
 */
final case class JobRef(namespace: String, name: String)

/**
 * OpenLineage run statuses. The wire format is the string value below; this
 * sealed trait just gives us exhaustive pattern matching at the call site.
 */
sealed abstract class RunStatus(val wireValue: String) extends Product with Serializable

object RunStatus {
  case object Start    extends RunStatus("START")
  case object Running  extends RunStatus("RUNNING")
  case object Complete extends RunStatus("COMPLETE")
  case object Fail     extends RunStatus("FAIL")
  case object Abort    extends RunStatus("ABORT")
  case object Other    extends RunStatus("OTHER")

  val all: Seq[RunStatus] = Seq(Start, Running, Complete, Fail, Abort, Other)

  def fromWire(s: String): Option[RunStatus] = all.find(_.wireValue.equalsIgnoreCase(s))
}
