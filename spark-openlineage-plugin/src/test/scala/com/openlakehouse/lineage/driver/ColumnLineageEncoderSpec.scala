package com.openlakehouse.lineage.driver

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for `ColumnLineageEncoder`. We don't go through Spark here:
 * the encoder is a pure mapping from Scala case classes to generated
 * Java protobuf builders.
 */
final class ColumnLineageEncoderSpec extends AnyFunSuite with Matchers {

  test("encode an empty facet produces a default-instance message") {
    val proto = ColumnLineageEncoder.encode(ColumnLineageFacet.Empty)
    proto.getFieldsCount  shouldBe 0
    proto.getDatasetCount shouldBe 0
  }

  test("encode a single-field IDENTITY mapping") {
    val facet = ColumnLineageFacet(
      fields = Map(
        "id" -> Seq(ColumnLineageInputField(
          namespace = "sales",
          name      = "orders",
          field     = "order_id",
          transformations = Seq(ColumnLineageTransformation.Identity)
        ))
      ),
      dataset = Seq.empty
    )

    val proto = ColumnLineageEncoder.encode(facet)
    proto.getFieldsMap.size shouldBe 1
    val out = proto.getFieldsMap.get("id")
    out                         should not be null
    out.getInputFieldsCount      shouldBe 1
    val in = out.getInputFields(0)
    in.getNamespace shouldBe "sales"
    in.getName      shouldBe "orders"
    in.getField     shouldBe "order_id"
    in.getTransformationsCount shouldBe 1
    val t = in.getTransformations(0)
    t.getType    shouldBe "DIRECT"
    t.getSubtype shouldBe "IDENTITY"
    t.getMasking shouldBe false
  }

  test("encode preserves transformation description and masking flag") {
    val facet = ColumnLineageFacet(
      fields = Map(
        "email_hash" -> Seq(ColumnLineageInputField(
          namespace = "sales",
          name      = "customers",
          field     = "email",
          transformations = Seq(ColumnLineageTransformation.transformation("md5", masking = true))
        ))
      ),
      dataset = Seq.empty
    )

    val proto = ColumnLineageEncoder.encode(facet)
    val t = proto.getFieldsMap.get("email_hash").getInputFields(0).getTransformations(0)
    t.getType        shouldBe "DIRECT"
    t.getSubtype     shouldBe "TRANSFORMATION"
    t.getDescription shouldBe "md5"
    t.getMasking     shouldBe true
  }

  test("dataset-level INDIRECT deps land in the dataset[] array") {
    val facet = ColumnLineageFacet(
      fields = Map.empty,
      dataset = Seq(
        ColumnLineageInputField("sales", "orders", "region",
          transformations = Seq(ColumnLineageTransformation.Filter)),
        ColumnLineageInputField("sales", "orders", "ts",
          transformations = Seq(ColumnLineageTransformation.Sort))
      )
    )
    val proto = ColumnLineageEncoder.encode(facet)
    proto.getDatasetCount shouldBe 2
    val deps = proto.getDatasetList.asScala.toList
    deps.map(_.getField) should contain theSameElementsInOrderAs Seq("region", "ts")
    deps.head.getTransformations(0).getSubtype shouldBe "FILTER"
    deps(1).getTransformations(0).getSubtype   shouldBe "SORT"
  }

  test("encoding multiple transformations on a single input field is preserved in order") {
    val facet = ColumnLineageFacet(
      fields = Map(
        "x" -> Seq(ColumnLineageInputField(
          namespace = "ns",
          name      = "t",
          field     = "x",
          transformations = Seq(
            ColumnLineageTransformation.transformation("upper"),
            ColumnLineageTransformation.transformation("trim")
          )
        ))
      ),
      dataset = Seq.empty
    )
    val proto = ColumnLineageEncoder.encode(facet)
    val ts = proto.getFieldsMap.get("x").getInputFields(0).getTransformationsList.asScala.toList
    ts.map(_.getDescription) shouldBe Seq("upper", "trim")
  }

  test("encode handles many fields without dropping entries") {
    val fields = (1 to 25).map(i =>
      s"f$i" -> Seq(ColumnLineageInputField("ns", "t", s"src$i",
        transformations = Seq(ColumnLineageTransformation.Identity)))
    ).toMap
    val proto = ColumnLineageEncoder.encode(ColumnLineageFacet(fields, Seq.empty))
    proto.getFieldsMap.size shouldBe 25
    (1 to 25).foreach { i =>
      proto.getFieldsMap.get(s"f$i").getInputFields(0).getField shouldBe s"src$i"
    }
  }
}
