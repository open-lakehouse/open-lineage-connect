package com.openlakehouse.lineage.driver

import scala.jdk.CollectionConverters._

import lineage.v1.Lineage

/**
 * Bridges the in-memory `ColumnLineageFacet` model into the generated Java
 * `lineage.v1.Lineage.ColumnLineageDatasetFacet` proto.
 *
 * Pure and side-effect free, mirroring the style of `FacetEncoder`. Empty
 * facets serialise as a default-instance message rather than `null`, so
 * callers can unconditionally read them back in tests; the proto's wire
 * encoding still elides empty maps and repeated fields, so this costs no
 * bytes on the wire.
 */
object ColumnLineageEncoder {

  def encode(facet: ColumnLineageFacet): Lineage.ColumnLineageDatasetFacet = {
    val builder = Lineage.ColumnLineageDatasetFacet.newBuilder()

    if (facet.fields.nonEmpty) {
      val javaMap = new java.util.LinkedHashMap[String, Lineage.OutputFieldLineage]()
      facet.fields.foreach { case (column, inputs) =>
        javaMap.put(column, encodeOutputField(inputs))
      }
      builder.putAllFields(javaMap)
    }

    if (facet.dataset.nonEmpty) {
      builder.addAllDataset(facet.dataset.map(encodeInputField).asJava)
    }

    builder.build()
  }

  private def encodeOutputField(
      inputs: Seq[ColumnLineageInputField]
  ): Lineage.OutputFieldLineage = {
    val b = Lineage.OutputFieldLineage.newBuilder()
    if (inputs.nonEmpty) b.addAllInputFields(inputs.map(encodeInputField).asJava)
    b.build()
  }

  private def encodeInputField(field: ColumnLineageInputField): Lineage.InputField = {
    val b = Lineage.InputField.newBuilder()
      .setNamespace(field.namespace)
      .setName(field.name)
      .setField(field.field)
    if (field.transformations.nonEmpty) {
      b.addAllTransformations(field.transformations.map(encodeTransformation).asJava)
    }
    b.build()
  }

  private def encodeTransformation(t: ColumnLineageTransformation): Lineage.FieldTransformation =
    Lineage.FieldTransformation.newBuilder()
      .setType(t.kind)
      .setSubtype(t.subtype)
      .setDescription(t.description)
      .setMasking(t.masking)
      .build()
}
