package com.openlakehouse.lineage.driver

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class FacetEncoderSpec extends AnyFunSuite with Matchers {

  test("encode empty map produces empty Struct") {
    val s = FacetEncoder.encode(Map.empty)
    s.getFieldsCount shouldBe 0
  }

  test("encode string map produces a Struct of string Values") {
    val s = FacetEncoder.encode(Map("format" -> "parquet", "path" -> "/tmp/a"))
    s.getFieldsCount shouldBe 2
    s.getFieldsOrThrow("format").getStringValue shouldBe "parquet"
    s.getFieldsOrThrow("path").getStringValue shouldBe "/tmp/a"
  }

  test("encodeMixed handles booleans, numbers, nested maps, and lists") {
    val s = FacetEncoder.encodeMixed(Map(
      "b"      -> true,
      "n"      -> 42,
      "nested" -> Map("x" -> "y"),
      "list"   -> Seq(1, 2, 3)
    ))

    s.getFieldsOrThrow("b").getBoolValue shouldBe true
    s.getFieldsOrThrow("n").getNumberValue shouldBe 42.0
    s.getFieldsOrThrow("nested").getStructValue.getFieldsOrThrow("x").getStringValue shouldBe "y"

    val list = s.getFieldsOrThrow("list").getListValue.getValuesList.asScala.toList
    list.map(_.getNumberValue.toInt) shouldBe List(1, 2, 3)
  }

  test("unknown types fall back to toString") {
    final case class Custom(v: String) { override def toString: String = s"Custom($v)" }
    val s = FacetEncoder.encodeMixed(Map("c" -> Custom("hi")))
    s.getFieldsOrThrow("c").getStringValue shouldBe "Custom(hi)"
  }
}
