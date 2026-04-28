package com.openlakehouse.lineage.driver

import java.time.Instant
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class RunEventBuilderSpec extends AnyFunSuite with Matchers {

  private val fixedTime = Instant.parse("2026-04-21T12:34:56.789Z")
  private val fixedUuid = UUID.fromString("11111111-2222-3333-4444-555555555555")

  private def ctx(
      status: RunStatus,
      inputs: Seq[DatasetRef] = Seq.empty,
      outputs: Seq[DatasetRef] = Seq.empty,
      executionId: Option[Long] = Some(7L),
      errorMessage: Option[String] = None,
      errorClass: Option[String] = None,
      facets: Map[String, String] = Map("funcName" -> "saveAsTable"),
      columnLineage: Map[(String, String), ColumnLineageFacet] = Map.empty
  ): RunContext =
    RunContext(
      runId        = fixedUuid,
      job          = JobRef("prod", "my_job"),
      eventTime    = fixedTime,
      status       = status,
      inputs       = inputs,
      outputs      = outputs,
      executionId  = executionId,
      errorMessage = errorMessage,
      errorClass   = errorClass,
      facets       = facets,
      columnLineage = columnLineage
    )

  private val builder =
    new RunEventBuilder(producerUri = "urn:test:producer", schemaUrl = "urn:test:schema")

  test("START event carries event_type=START, the run_id, job name, and timestamp") {
    val ev = builder.build(ctx(RunStatus.Start))
    ev.getEventType shouldBe "START"
    ev.getProducer  shouldBe "urn:test:producer"
    ev.getSchemaUrl shouldBe "urn:test:schema"
    ev.getEventTime.getSeconds shouldBe fixedTime.getEpochSecond
    ev.getEventTime.getNanos   shouldBe fixedTime.getNano
    ev.getRun.getRunId         shouldBe fixedUuid.toString
    ev.getJob.getNamespace     shouldBe "prod"
    ev.getJob.getName          shouldBe "my_job"
  }

  test("Run facets include executionId and caller-provided facets") {
    val ev = builder.build(ctx(RunStatus.Start))
    val f  = ev.getRun.getFacets
    f.getFieldsOrThrow("executionId").getStringValue shouldBe "7"
    f.getFieldsOrThrow("funcName").getStringValue    shouldBe "saveAsTable"
  }

  test("FAIL event carries errorClass + errorMessage in Run facets") {
    val ev = builder.build(ctx(
      RunStatus.Fail,
      errorClass   = Some("java.lang.RuntimeException"),
      errorMessage = Some("boom")
    ))
    ev.getEventType shouldBe "FAIL"
    val f = ev.getRun.getFacets
    f.getFieldsOrThrow("errorClass").getStringValue   shouldBe "java.lang.RuntimeException"
    f.getFieldsOrThrow("errorMessage").getStringValue shouldBe "boom"
  }

  test("inputs and outputs are mapped with sourceType facet injected") {
    val src = DatasetRef("file", "/data/in", "parquet", facets = Map("format" -> "parquet"))
    val snk = DatasetRef("file", "/data/out", "parquet", facets = Map("writeMode" -> "overwrite"))
    val ev  = builder.build(ctx(RunStatus.Complete, inputs = Seq(src), outputs = Seq(snk)))

    ev.getInputsCount  shouldBe 1
    ev.getOutputsCount shouldBe 1

    val in = ev.getInputs(0)
    in.getNamespace shouldBe "file"
    in.getName      shouldBe "/data/in"
    in.getFacets.getFieldsOrThrow("sourceType").getStringValue shouldBe "parquet"
    in.getFacets.getFieldsOrThrow("format").getStringValue     shouldBe "parquet"

    val out = ev.getOutputs(0)
    out.getNamespace shouldBe "file"
    out.getName      shouldBe "/data/out"
    out.getFacets.getFieldsOrThrow("sourceType").getStringValue shouldBe "parquet"
    out.getFacets.getFieldsOrThrow("writeMode").getStringValue  shouldBe "overwrite"
  }

  test("datasets with no facets produce no facets Struct") {
    val src = DatasetRef("cat", "db.t", "iceberg")
    val ev  = builder.build(ctx(RunStatus.Start, inputs = Seq(src)))
    ev.getInputs(0).hasFacets shouldBe false
  }

  test("empty inputs/outputs produce no dataset entries") {
    val ev = builder.build(ctx(RunStatus.Start))
    ev.getInputsList.asScala.toList  shouldBe empty
    ev.getOutputsList.asScala.toList shouldBe empty
  }

  test("column lineage attaches to outputs by (namespace, name)") {
    val out = DatasetRef("file", "/data/out", "parquet")
    val facet = ColumnLineageFacet(
      fields = Map("id" -> Seq(ColumnLineageInputField("file", "/data/in", "id",
        transformations = Seq(ColumnLineageTransformation.Identity)))),
      dataset = Seq.empty
    )
    val ev = builder.build(ctx(
      RunStatus.Complete,
      outputs       = Seq(out),
      columnLineage = Map(out.identityKey -> facet)
    ))

    ev.getOutputsCount shouldBe 1
    val o = ev.getOutputs(0)
    o.hasColumnLineage shouldBe true
    o.getColumnLineage.getFieldsMap.size shouldBe 1
    val cl = o.getColumnLineage.getFieldsMap.get("id")
    cl.getInputFieldsCount shouldBe 1
    cl.getInputFields(0).getNamespace shouldBe "file"
    cl.getInputFields(0).getField     shouldBe "id"
  }

  test("output dataset without a matching column-lineage entry leaves the field unset") {
    val out = DatasetRef("file", "/data/out", "parquet")
    val ev  = builder.build(ctx(RunStatus.Complete, outputs = Seq(out)))
    ev.getOutputs(0).hasColumnLineage shouldBe false
  }

  test("column lineage attaches to inputs when keyed by an input dataset's identity") {
    val in = DatasetRef("file", "/data/in", "parquet")
    val facet = ColumnLineageFacet(
      fields = Map.empty,
      dataset = Seq(ColumnLineageInputField("file", "/data/in", "ts",
        transformations = Seq(ColumnLineageTransformation.Sort)))
    )
    val ev = builder.build(ctx(
      RunStatus.Start,
      inputs        = Seq(in),
      columnLineage = Map(in.identityKey -> facet)
    ))

    val i = ev.getInputs(0)
    i.hasColumnLineage shouldBe true
    i.getColumnLineage.getDatasetCount shouldBe 1
    i.getColumnLineage.getDataset(0).getField shouldBe "ts"
  }
}
