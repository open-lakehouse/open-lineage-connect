package com.openlakehouse.lineage.streaming

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class StreamingSourceParserSpec extends AnyFunSuite with Matchers {

  test("FileStreamSource with a file URI splits into scheme-authority namespace and path") {
    val ref = StreamingSourceParser.parseSource("FileStreamSource[file:/tmp/data]")
    ref.namespace shouldBe "file"
    ref.name shouldBe "/tmp/data"
    ref.sourceType shouldBe "FileStreamSource"
  }

  test("FileStreamSource with an s3a URI keeps the bucket as authority") {
    val ref = StreamingSourceParser.parseSource("FileStreamSource[s3a://my-bucket/events/year=2025]")
    ref.namespace shouldBe "s3a://my-bucket"
    ref.name shouldBe "/events/year=2025"
    ref.sourceType shouldBe "FileStreamSource"
  }

  test("KafkaV2 Subscribe parses the topic list") {
    val ref = StreamingSourceParser.parseSource("KafkaV2[Subscribe[topic-a,topic-b]]")
    ref.namespace shouldBe "kafka"
    ref.name shouldBe "topic-a,topic-b"
    ref.sourceType shouldBe "KafkaSource"
    ref.facets.get("topics") shouldBe Some("topic-a,topic-b")
  }

  test("KafkaV2 SubscribePattern falls back to the raw body") {
    val ref = StreamingSourceParser.parseSource("KafkaV2[SubscribePattern[orders-.*]]")
    ref.namespace shouldBe "kafka"
    ref.name shouldBe "orders-.*"
  }

  test("Delta source with a path is treated as a file") {
    val ref = StreamingSourceParser.parseSource("DeltaSource[file:/tmp/delta/orders]")
    ref.namespace shouldBe "file"
    ref.name shouldBe "/tmp/delta/orders"
    ref.sourceType shouldBe "DeltaSource"
  }

  test("Delta source with a bare table name is retained as-is") {
    val ref = StreamingSourceParser.parseSource("DeltaSource[main.lakehouse.orders]")
    ref.namespace shouldBe "delta"
    ref.name shouldBe "main.lakehouse.orders"
  }

  test("FileSink round-trips sinks distinctly from sources") {
    val src  = StreamingSourceParser.parseSource("FileSink[s3a://bucket/out]")
    val sink = StreamingSourceParser.parseSink("FileSink[s3a://bucket/out]")
    src.sourceType should not equal sink.sourceType
    sink.sourceType shouldBe "FileSink"
  }

  test("Unknown description is preserved as the raw name") {
    val ref = StreamingSourceParser.parseSource("SomethingWeird:42")
    ref.namespace shouldBe "streaming"
    ref.name shouldBe "SomethingWeird:42"
  }

  test("Empty description is mapped to unknown") {
    val ref = StreamingSourceParser.parseSource("")
    ref.name shouldBe "unknown"
  }
}
