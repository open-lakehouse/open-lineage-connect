package com.openlakehouse.lineage.driver

import java.time.Instant

import scala.jdk.CollectionConverters._

import com.google.protobuf.Timestamp

import lineage.v1.Lineage

/**
 * Converts a `RunContext` into a wire-ready `lineage.v1.RunEvent` proto.
 *
 * Kept pure and side-effect free: no I/O, no logging, no Spark dependencies.
 * This is the narrow contract between the driver's in-memory run model and
 * the ConnectRPC emitter. The emitter only ever sees proto bytes.
 *
 * Schema URL + producer URI are constants, parameterized at construction so
 * tests can override them without caring about git metadata.
 */
final class RunEventBuilder(
    producerUri: String  = RunEventBuilder.DefaultProducerUri,
    schemaUrl: String    = RunEventBuilder.DefaultSchemaUrl
) {

  def build(ctx: RunContext): Lineage.RunEvent = {
    val builder = Lineage.RunEvent.newBuilder()
      .setEventType(ctx.status.wireValue)
      .setEventTime(toProtoTimestamp(ctx.eventTime))
      .setProducer(producerUri)
      .setSchemaUrl(schemaUrl)
      .setRun(buildRun(ctx))
      .setJob(buildJob(ctx.job))

    val inputs = ctx.inputs.map(toInputDataset).asJava
    if (!inputs.isEmpty) builder.addAllInputs(inputs)

    val outputs = ctx.outputs.map(toOutputDataset).asJava
    if (!outputs.isEmpty) builder.addAllOutputs(outputs)

    builder.build()
  }

  private def buildRun(ctx: RunContext): Lineage.Run = {
    val facets = ctx.facets ++
      ctx.executionId.map("executionId" -> _.toString) ++
      ctx.errorClass.map("errorClass" -> _) ++
      ctx.errorMessage.map("errorMessage" -> _)
    Lineage.Run.newBuilder()
      .setRunId(ctx.runId.toString)
      .setFacets(FacetEncoder.encode(facets))
      .build()
  }

  private def buildJob(j: JobRef): Lineage.Job =
    Lineage.Job.newBuilder()
      .setNamespace(j.namespace)
      .setName(j.name)
      .build()

  private def toInputDataset(d: DatasetRef): Lineage.InputDataset = {
    val b = Lineage.InputDataset.newBuilder()
      .setNamespace(d.namespace)
      .setName(d.name)
    if (d.facets.nonEmpty) b.setFacets(FacetEncoder.encode(d.facets + ("sourceType" -> d.sourceType)))
    b.build()
  }

  private def toOutputDataset(d: DatasetRef): Lineage.OutputDataset = {
    val b = Lineage.OutputDataset.newBuilder()
      .setNamespace(d.namespace)
      .setName(d.name)
    if (d.facets.nonEmpty) b.setFacets(FacetEncoder.encode(d.facets + ("sourceType" -> d.sourceType)))
    b.build()
  }

  private def toProtoTimestamp(i: Instant): Timestamp =
    Timestamp.newBuilder()
      .setSeconds(i.getEpochSecond)
      .setNanos(i.getNano)
      .build()
}

object RunEventBuilder {
  // These are project-wide identity markers for the lineage graph, not secrets.
  // Kept here (rather than BuildInfo) so tests can construct events deterministically.
  val DefaultProducerUri: String =
    "https://github.com/open-lakehouse/open-lineage-service/spark-openlineage-plugin"
  val DefaultSchemaUrl: String =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json"
}
