package com.openlakehouse.lineage.driver

import scala.annotation.nowarn
import scala.util.Try

import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.{
  CreateTableAsSelect,
  LogicalPlan,
  ReplaceTableAsSelect,
  V2WriteCommand
}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.v2.{
  DataSourceV2Relation,
  DataSourceV2ScanRelation,
  WriteToDataSourceV2
}
import org.apache.spark.sql.execution.datasources.{
  HadoopFsRelation,
  InsertIntoHadoopFsRelationCommand,
  LogicalRelation,
  SaveIntoDataSourceCommand
}
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSource

/**
 * Walks an analyzed `LogicalPlan` and extracts `DatasetRef`s for lineage sources
 * (and, in a subsequent change, sinks).
 *
 * This is the canonical source of truth for "what tables/paths did this query
 * read/write." All matching is intentionally resilient:
 *
 *   - Reads `qe.analyzed` (resolved) plans. `UnresolvedRelation` is still matched
 *     as a fallback so that misconfigured listener placements don't drop events.
 *   - Every extractor returns `Option[DatasetRef]`, and unknown relation types
 *     fall through to a best-effort "unknown" ref keyed on class name rather
 *     than being silently discarded.
 *   - No throwing. Any unexpected shape is either handled or returns `None`.
 *
 * See `COMPAT.md` for the exact Spark 4.1.1 class/accessor signatures this
 * relies on.
 */
object QueryPlanVisitor {

  /**
   * Collect all source datasets referenced by `plan`, deduplicated by
   * `(namespace, name)`.
   */
  def extractSources(plan: LogicalPlan): Seq[DatasetRef] = {
    val direct = plan.collect {
      case l: LogicalRelation          => fromLogicalRelation(l)
      case h: HiveTableRelation        => Some(fromHiveTableRelation(h))
      case v: DataSourceV2Relation     => Some(fromV2Relation(v))
      case s: DataSourceV2ScanRelation => Some(fromV2Relation(s.relation))
      case u: UnresolvedRelation       => fromUnresolvedRelation(u)
    }.flatten

    // Some write commands in analyzed plans do not expose their source scans as
    // direct children. Pull from embedded query plans when available.
    val nestedFromWrites = plan.collect {
      case c: InsertIntoHadoopFsRelationCommand    => extractSources(c.query)
      case c: SaveIntoDataSourceCommand            => extractSources(c.query)
      case c: CreateDataSourceTableAsSelectCommand => extractSources(c.query)
      case c: CreateTableAsSelect                  => extractSources(c.query)
      case c: ReplaceTableAsSelect                 => extractSources(c.query)
    }.flatten

    DatasetRef.distinctByIdentity(direct ++ nestedFromWrites)
  }

  /**
   * Collect all sink datasets written by `plan`, deduplicated by
   * `(namespace, name)`.
   *
   * The match order below is significant only for nodes whose types overlap
   * through inheritance (e.g. `CreateTableAsSelect` does **not** extend
   * `V2WriteCommand`, so those two cases are independent). Each concrete write
   * node produces at most one ref.
   */
  @nowarn("cat=deprecation")  // WriteToDataSourceV2 is deprecated since 2.4 but still present; kept as fallback.
  def extractSinks(plan: LogicalPlan): Seq[DatasetRef] = {
    val collected = plan.collect {
      // ---- V1 write commands -----------------------------------------------
      case c: InsertIntoHadoopFsRelationCommand    => fromInsertIntoHadoopFs(c)
      case c: SaveIntoDataSourceCommand            => fromSaveIntoDataSource(c)
      case c: CreateDataSourceTableAsSelectCommand => Some(fromCreateDataSourceTas(c))

      // ---- V2 CTAS/RTAS (separate trait from V2WriteCommand) --------------
      case c: CreateTableAsSelect                  => fromV2CreateTableAsSelect(c)
      case c: ReplaceTableAsSelect                 => fromV2ReplaceTableAsSelect(c)

      // ---- V2 writes (AppendData, OverwriteByExpression, ReplaceData, ...) -
      case c: V2WriteCommand                       => Some(fromV2WriteCommand(c))

      // ---- Lower-level v2 writes ------------------------------------------
      case c: WriteToDataSourceV2                  => fromWriteToDataSourceV2(c)
      case c: WriteToMicroBatchDataSource          => fromWriteToMicroBatch(c)
    }.flatten

    // Reflective fallback for connectors whose write commands are off the main
    // classpath (Delta/Iceberg/Hudi). In Spark 4.x those typically lower to
    // V2WriteCommand and are already caught above, but we keep this as defence.
    val reflective = ReflectiveSinkMatchers.matchAll(plan)

    DatasetRef.distinctByIdentity(collected ++ reflective)
  }

  // --------------------------------------------------------------------------
  // V1: LogicalRelation (+ HadoopFsRelation, JDBC, etc.)
  // --------------------------------------------------------------------------

  private[driver] def fromLogicalRelation(l: LogicalRelation): Option[DatasetRef] = {
    // Prefer the catalog table when we have one — it's the strongest identity.
    l.catalogTable match {
      case Some(ct) =>
        Some(DatasetRef(
          namespace  = ct.identifier.database.getOrElse("default"),
          name       = ct.identifier.table,
          sourceType = ct.provider.getOrElse("unknown"),
          facets     = Map(
            "catalog"    -> "session",
            "isStreaming" -> l.isStreaming.toString
          )
        ))
      case None =>
        l.relation match {
          case h: HadoopFsRelation =>
            h.location.rootPaths.headOption.map { path =>
              val uri = path.toUri
              val scheme    = Option(uri.getScheme).getOrElse("file")
              val authority = Option(uri.getAuthority).getOrElse("")
              val pathStr   = Option(uri.getPath).getOrElse(path.toString)
              DatasetRef(
                namespace  = s"$scheme://$authority",
                name       = pathStr,
                sourceType = stripScalaObjectSuffix(h.fileFormat.getClass.getSimpleName)
              )
            }
          case other =>
            // JDBC / Kafka-as-v1 / custom BaseRelation — no universal identity.
            // Emit a best-effort marker so we at least know a source is present.
            Some(DatasetRef(
              namespace  = "unknown",
              name       = other.getClass.getSimpleName,
              sourceType = other.getClass.getSimpleName
            ))
        }
    }
  }

  // --------------------------------------------------------------------------
  // V1: HiveTableRelation
  // --------------------------------------------------------------------------

  private[driver] def fromHiveTableRelation(h: HiveTableRelation): DatasetRef = {
    val id = h.tableMeta.identifier
    DatasetRef(
      namespace  = id.database.getOrElse("default"),
      name       = id.table,
      sourceType = h.tableMeta.provider.getOrElse("hive"),
      facets     = Map("catalog" -> "hive")
    )
  }

  // --------------------------------------------------------------------------
  // V2: DataSourceV2Relation / DataSourceV2ScanRelation
  // --------------------------------------------------------------------------

  private[driver] def fromV2Relation(v: DataSourceV2Relation): DatasetRef = {
    val tableClass = stripScalaObjectSuffix(v.table.getClass.getSimpleName)

    (v.identifier, v.catalog) match {
      case (Some(id), Some(cat)) =>
        // Fully-resolved catalog identifier — this is the strongest name.
        val ns    = id.namespace()
        val nsStr = if (ns == null || ns.isEmpty) cat.name() else s"${cat.name()}.${ns.mkString(".")}"
        DatasetRef(
          namespace  = nsStr,
          name       = id.name(),
          sourceType = tableClass,
          facets     = Map("catalog" -> cat.name())
        )

      case (Some(id), None) =>
        val ns    = id.namespace()
        val nsStr = if (ns == null || ns.isEmpty) "default" else ns.mkString(".")
        DatasetRef(
          namespace  = nsStr,
          name       = id.name(),
          sourceType = tableClass
        )

      case _ =>
        // Path-based loads (e.g. spark.read.format("iceberg").load("s3://...")) expose
        // themselves only via options("path").
        Option(v.options.get("path")) match {
          case Some(p) =>
            DatasetRef(namespace = "path", name = p, sourceType = tableClass)
          case None =>
            DatasetRef(namespace = "v2", name = Option(v.table.name()).getOrElse("anonymous"), sourceType = tableClass)
        }
    }
  }

  // --------------------------------------------------------------------------
  // Fallback: UnresolvedRelation
  //
  // This should not fire when listening on analyzed plans, but we keep it for
  // safety so a misconfigured listener still produces some lineage signal.
  // --------------------------------------------------------------------------

  private[driver] def fromUnresolvedRelation(u: UnresolvedRelation): Option[DatasetRef] = {
    u.multipartIdentifier.toList match {
      case Nil            => None
      case one :: Nil     => Some(DatasetRef("default", one, "unresolved"))
      case parts          => Some(DatasetRef(parts.init.mkString("."), parts.last, "unresolved"))
    }
  }

  // --------------------------------------------------------------------------
  // Sinks: V1
  // --------------------------------------------------------------------------

  private[driver] def fromInsertIntoHadoopFs(c: InsertIntoHadoopFsRelationCommand): Option[DatasetRef] = {
    // Prefer the catalog table (stronger identity) when present; otherwise the
    // raw output path.
    c.catalogTable match {
      case Some(ct) =>
        Some(datasetRefFromCatalogTable(ct, extraFacets = Map("saveMode" -> c.mode.toString)))
      case None =>
        val uri        = c.outputPath.toUri
        val scheme     = Option(uri.getScheme).getOrElse("file")
        val authority  = Option(uri.getAuthority).getOrElse("")
        val pathStr    = Option(uri.getPath).getOrElse(c.outputPath.toString)
        val formatName = stripScalaObjectSuffix(c.fileFormat.getClass.getSimpleName)
        Some(DatasetRef(
          namespace  = s"$scheme://$authority",
          name       = pathStr,
          sourceType = formatName,
          facets     = Map("saveMode" -> c.mode.toString)
        ))
    }
  }

  private[driver] def fromSaveIntoDataSource(c: SaveIntoDataSourceCommand): Option[DatasetRef] = {
    val providerClass = stripScalaObjectSuffix(c.dataSource.getClass.getSimpleName)
    c.options.get("path") match {
      case Some(p) =>
        Some(DatasetRef(
          namespace  = "path",
          name       = p,
          sourceType = providerClass,
          facets     = Map("saveMode" -> c.mode.toString)
        ))
      case None =>
        // JDBC / custom providers without a path — best effort.
        val jdbcUrl = c.options.get("url")
        val dbtable = c.options.get("dbtable").orElse(c.options.get("query")).getOrElse("unknown")
        jdbcUrl match {
          case Some(url) =>
            Some(DatasetRef(
              namespace  = url,
              name       = dbtable,
              sourceType = providerClass,
              facets     = Map("saveMode" -> c.mode.toString)
            ))
          case None =>
            Some(DatasetRef(
              namespace  = "unknown",
              name       = providerClass,
              sourceType = providerClass,
              facets     = Map("saveMode" -> c.mode.toString)
            ))
        }
    }
  }

  private[driver] def fromCreateDataSourceTas(c: CreateDataSourceTableAsSelectCommand): DatasetRef =
    datasetRefFromCatalogTable(c.table, extraFacets = Map("saveMode" -> c.mode.toString, "ctas" -> "true"))

  private[driver] def datasetRefFromCatalogTable(
      ct: CatalogTable,
      extraFacets: Map[String, String] = Map.empty
  ): DatasetRef = {
    val id       = ct.identifier
    val location = ct.storage.locationUri.map(_.toString)
    val base     = Map("catalog" -> "session") ++ location.map("location" -> _)
    DatasetRef(
      namespace  = id.database.getOrElse("default"),
      name       = id.table,
      sourceType = ct.provider.getOrElse("unknown"),
      facets     = base ++ extraFacets
    )
  }

  // --------------------------------------------------------------------------
  // Sinks: V2
  // --------------------------------------------------------------------------

  private[driver] def fromV2WriteCommand(c: V2WriteCommand): DatasetRef = {
    val ref = fromNamedRelation(c.table)
    ref.copy(facets = ref.facets ++ Map(
      "writeKind" -> stripScalaObjectSuffix(c.getClass.getSimpleName),
      "isByName"  -> c.isByName.toString
    ))
  }

  private[driver] def fromV2CreateTableAsSelect(c: CreateTableAsSelect): Option[DatasetRef] =
    resolvedIdentifierToRef(c.name, facets = Map(
      "writeKind"      -> "CreateTableAsSelect",
      "ignoreIfExists" -> c.ignoreIfExists.toString
    ))

  private[driver] def fromV2ReplaceTableAsSelect(c: ReplaceTableAsSelect): Option[DatasetRef] =
    resolvedIdentifierToRef(c.name, facets = Map("writeKind" -> "ReplaceTableAsSelect"))

  /**
   * V2 CTAS/RTAS carry their target in `.name: LogicalPlan`, which after
   * analysis is a `ResolvedIdentifier`. Pre-analysis plans will skip cleanly.
   */
  private def resolvedIdentifierToRef(
      namePlan: LogicalPlan,
      facets: Map[String, String]
  ): Option[DatasetRef] = namePlan match {
    case ResolvedIdentifier(cat, id) =>
      val ns     = Option(id.namespace()).getOrElse(Array.empty[String])
      val nsStr  = if (ns.isEmpty) cat.name() else s"${cat.name()}.${ns.mkString(".")}"
      Some(DatasetRef(
        namespace  = nsStr,
        name       = id.name(),
        sourceType = "v2",
        facets     = facets ++ Map("catalog" -> cat.name())
      ))
    case _ => None
  }

  @nowarn("cat=deprecation")
  private[driver] def fromWriteToDataSourceV2(c: WriteToDataSourceV2): Option[DatasetRef] =
    c.relation.map { r =>
      val ref = fromV2Relation(r)
      ref.copy(facets = ref.facets + ("writeKind" -> "WriteToDataSourceV2"))
    }

  private[driver] def fromWriteToMicroBatch(c: WriteToMicroBatchDataSource): Option[DatasetRef] = {
    val baseOpt = c.relation.map(fromV2Relation).orElse {
      Option(c.table.name()).map { n =>
        DatasetRef(namespace = "streaming", name = n, sourceType = stripScalaObjectSuffix(c.table.getClass.getSimpleName))
      }
    }
    baseOpt.map(r => r.copy(facets = r.facets ++ Map(
      "writeKind"  -> "WriteToMicroBatchDataSource",
      "queryId"    -> c.queryId,
      "outputMode" -> c.outputMode.toString
    )))
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  /**
   * Lift a `NamedRelation` into a `DatasetRef`. Falls back to `.name()` when
   * the underlying type isn't a recognised relation.
   */
  private def fromNamedRelation(n: NamedRelation): DatasetRef = n match {
    case v: DataSourceV2Relation     => fromV2Relation(v)
    case v: DataSourceV2ScanRelation => fromV2Relation(v.relation)
    case other                       =>
      val name       = Try(other.name).getOrElse("anonymous")
      val parts      = name.split('.').toList
      val (ns, leaf) = parts match {
        case Nil        => ("default", name)
        case one :: Nil => ("default", one)
        case more       => (more.init.mkString("."), more.last)
      }
      DatasetRef(
        namespace  = ns,
        name       = leaf,
        sourceType = stripScalaObjectSuffix(other.getClass.getSimpleName)
      )
  }

  // --------------------------------------------------------------------------
  // Utility: Scala object class names end with '$'.
  // --------------------------------------------------------------------------

  private def stripScalaObjectSuffix(name: String): String =
    if (name.endsWith("$")) name.dropRight(1) else name
}

/**
 * Convenience view on a `DatasetRef` used by callers that just want to know
 * "what datasets does this plan touch" without caring about source/sink split.
 */
final case class DatasetRefs(inputs: Seq[DatasetRef], outputs: Seq[DatasetRef]) {
  def isEmpty: Boolean   = inputs.isEmpty && outputs.isEmpty
  def nonEmpty: Boolean  = !isEmpty
}

object DatasetRefs {
  def fromPlan(plan: LogicalPlan): DatasetRefs =
    DatasetRefs(
      inputs  = QueryPlanVisitor.extractSources(plan),
      outputs = QueryPlanVisitor.extractSinks(plan)
    )
}
