package com.openlakehouse.lineage.driver

import com.google.protobuf.{ListValue, NullValue, Struct, Value}

/**
 * Encodes Scala facet maps into `google.protobuf.Struct` for the
 * `lineage.v1` proto messages (Run.facets, Job.facets, {Input,Output}Dataset.facets).
 *
 * `Struct` is protobuf's JSON-shaped container: a map of string keys to
 * heterogeneous `Value`s (null, boolean, number, string, list, or nested
 * struct). We stick to a small and well-defined subset so downstream
 * Go/Rust readers don't have to handle arbitrary JSON shapes.
 */
object FacetEncoder {

  def encode(facets: Map[String, String]): Struct = {
    val builder = Struct.newBuilder()
    facets.foreach { case (k, v) => builder.putFields(k, stringValue(v)) }
    builder.build()
  }

  def encodeMixed(facets: Map[String, Any]): Struct = {
    val builder = Struct.newBuilder()
    facets.foreach { case (k, v) => builder.putFields(k, toValue(v)) }
    builder.build()
  }

  def stringValue(s: String): Value = Value.newBuilder().setStringValue(s).build()
  def boolValue(b: Boolean): Value  = Value.newBuilder().setBoolValue(b).build()
  def numberValue(n: Double): Value = Value.newBuilder().setNumberValue(n).build()
  def nullValue: Value              = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()

  private def toValue(any: Any): Value = any match {
    case null           => nullValue
    case s: String      => stringValue(s)
    case b: Boolean     => boolValue(b)
    case i: Int         => numberValue(i.toDouble)
    case l: Long        => numberValue(l.toDouble)
    case d: Double      => numberValue(d)
    case f: Float       => numberValue(f.toDouble)
    case m: Map[_, _]   => Value.newBuilder().setStructValue(encodeMixed(m.map { case (k, v) => k.toString -> v }.toMap)).build()
    case seq: Seq[_]    =>
      val lv = ListValue.newBuilder()
      seq.foreach(v => lv.addValues(toValue(v)))
      Value.newBuilder().setListValue(lv.build()).build()
    case other          => stringValue(other.toString)
  }
}
