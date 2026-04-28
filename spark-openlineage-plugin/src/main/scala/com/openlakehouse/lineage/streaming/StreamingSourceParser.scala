package com.openlakehouse.lineage.streaming

import scala.util.matching.Regex

import com.openlakehouse.lineage.driver.DatasetRef

/**
 * Parses `description` strings from `SourceProgress` / `SinkProgress` into
 * `DatasetRef` values.
 *
 * The description field is Spark's free-form, human-readable label for a
 * streaming source/sink. It's the only identification we get at the
 * `StreamingQueryListener` level — the physical plan is not exposed. We
 * handle the common cases defensively:
 *
 *   - `FileStreamSource[<uri>]`
 *   - `FileSink[<uri>]`
 *   - `KafkaV2[Subscribe[topic1,topic2]]`
 *   - `KafkaV2[SubscribePattern[...]]`
 *   - `DeltaSource[...]` (path or table)
 *   - `DeltaSink[...]`
 *   - Anything unrecognized → a `DatasetRef` with `namespace="streaming"` and
 *     `name=<raw description>`, so the event is still emitted.
 */
object StreamingSourceParser {

  private val Bracketed: Regex       = """^([A-Za-z0-9_\.]+)\[(.*)\]$""".r
  private val KafkaSubscribe: Regex  = """^Subscribe(?:Pattern)?\[(.*)\]$""".r
  private val FileUri: Regex         = """^((?:file|hdfs|s3|s3a|gs|abfss?):/+.*)$""".r

  /** Parse a source description. */
  def parseSource(description: String): DatasetRef =
    parse(description, isSink = false)

  /** Parse a sink description. */
  def parseSink(description: String): DatasetRef =
    parse(description, isSink = true)

  private def parse(desc: String, isSink: Boolean): DatasetRef = {
    val trimmed = Option(desc).map(_.trim).getOrElse("")
    trimmed match {
      case "" =>
        DatasetRef("streaming", "unknown", sourceType = if (isSink) "sink" else "source")

      case Bracketed(kind, body) if kind.startsWith("FileStream") || kind == "FileSink" =>
        fileRef(kind, body, isSink)

      case Bracketed("KafkaV2", body) =>
        kafkaRef(body, isSink)

      case Bracketed(kind, body) if kind.toLowerCase.contains("kafka") =>
        kafkaRef(body, isSink)

      case Bracketed(kind, body) if kind.toLowerCase.contains("delta") =>
        deltaRef(body, isSink)

      case Bracketed(kind, body) if kind.toLowerCase.contains("rate") =>
        DatasetRef("streaming", s"rate:${kind.toLowerCase}", sourceType = if (isSink) "sink" else "source",
          facets = Map("raw" -> body))

      case _ =>
        // Last resort: use the raw description as the dataset name so nothing
        // is silently dropped. Operators can still correlate via Spark UI.
        DatasetRef("streaming", trimmed, sourceType = if (isSink) "sink" else "source")
    }
  }

  private def fileRef(kind: String, body: String, isSink: Boolean): DatasetRef = body match {
    case FileUri(uri) =>
      val (namespace, name) = splitUri(uri)
      DatasetRef(namespace, name,
        sourceType = if (isSink) "FileSink" else "FileStreamSource",
        facets = Map("description" -> s"$kind[$body]"))
    case _ =>
      DatasetRef("file", body, sourceType = if (isSink) "FileSink" else "FileStreamSource",
        facets = Map("description" -> s"$kind[$body]"))
  }

  private def kafkaRef(body: String, isSink: Boolean): DatasetRef = body match {
    case KafkaSubscribe(topics) =>
      val topicList = topics.split(",").map(_.trim).filter(_.nonEmpty).toList
      val name      = topicList.mkString(",")
      DatasetRef("kafka", if (name.isEmpty) "unknown" else name,
        sourceType = if (isSink) "KafkaSink" else "KafkaSource",
        facets = Map("topics" -> topicList.mkString(",")))
    case _ =>
      DatasetRef("kafka", body, sourceType = if (isSink) "KafkaSink" else "KafkaSource")
  }

  private def deltaRef(body: String, isSink: Boolean): DatasetRef = body match {
    case FileUri(uri) =>
      val (namespace, name) = splitUri(uri)
      DatasetRef(namespace, name,
        sourceType = if (isSink) "DeltaSink" else "DeltaSource",
        facets = Map("description" -> body))
    case _ =>
      // Might be a table name like `catalog.schema.table`.
      DatasetRef("delta", body, sourceType = if (isSink) "DeltaSink" else "DeltaSource")
  }

  /**
   * Split a URI into `(namespace, path)`. Uses `java.net.URI` so we get the
   * same parsing rules as Spark/Hadoop:
   *   - `file:/tmp/data`           → ("file", "/tmp/data")
   *   - `s3a://bucket/events`      → ("s3a://bucket", "/events")
   *   - `hdfs://nn:9000/warehouse` → ("hdfs://nn:9000", "/warehouse")
   *
   * Degrades to the raw string under the `file` namespace if the URI can't
   * be parsed — better a fuzzy name than a crash on malformed input.
   */
  private def splitUri(uri: String): (String, String) = {
    try {
      val u       = new java.net.URI(uri)
      val scheme  = Option(u.getScheme).filter(_.nonEmpty).getOrElse("file")
      val path    = Option(u.getRawPath).filter(_.nonEmpty).getOrElse("/")
      val authority = Option(u.getRawAuthority).filter(_.nonEmpty)
      val namespace = authority match {
        case Some(auth) => s"$scheme://$auth"
        case None       => scheme
      }
      (namespace, path)
    } catch {
      case _: Exception => ("file", uri)
    }
  }
}
