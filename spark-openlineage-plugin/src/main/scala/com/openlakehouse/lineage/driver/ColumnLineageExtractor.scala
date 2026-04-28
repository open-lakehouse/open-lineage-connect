package com.openlakehouse.lineage.driver

import scala.annotation.nowarn
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  ExprId,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  CreateTableAsSelect,
  Filter,
  Generate,
  Join,
  LogicalPlan,
  Project,
  ReplaceTableAsSelect,
  Sort,
  SubqueryAlias,
  Union,
  V2WriteCommand,
  Window
}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{
  InsertIntoHadoopFsRelationCommand,
  LogicalRelation,
  SaveIntoDataSourceCommand
}
import org.apache.spark.sql.execution.datasources.v2.{
  DataSourceV2Relation,
  DataSourceV2ScanRelation,
  WriteToDataSourceV2
}
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSource

import com.openlakehouse.lineage.driver.ColumnLineageInputField.{merge => mergeFields, mergeAll, withTransformation}
import com.openlakehouse.lineage.driver.ColumnLineageTransformation.{
  Filter => FilterTrans,
  GroupBy,
  Identity,
  Join => JoinTrans,
  Sort => SortTrans,
  Window => WindowTrans,
  aggregation,
  transformation
}

/**
 * Walks an analyzed `LogicalPlan` and computes a per-output-dataset
 * `ColumnLineageFacet` describing how each sink column was derived from
 * source columns, plus dataset-level dependencies (filters, joins,
 * group-by columns, sort keys, window partition/order columns).
 *
 * Algorithm overview:
 *
 *   1. Visit the plan bottom-up. For each node, return:
 *        - `attrLineage`: a map from `ExprId` of an attribute the node
 *           emits to the set of source-field inputs that contribute to it
 *           directly (DIRECT lineage).
 *        - `indirect`: dataset-wide INDIRECT dependencies introduced by
 *           filters, joins, group-by keys, sort keys, window
 *           partition/order specs, etc.
 *   2. At each sink, look up the sink's "query" sub-plan. The `output`
 *      attributes of that sub-plan, mapped against the sink's target
 *      column names, give us the per-field lineage.
 *
 * Fail-open by design: any unexpected plan shape returns an empty facet
 * for that sink rather than throwing. Callers wrap this in `safeExtract`
 * which is itself wrapped in `try/catch NonFatal`.
 *
 * Reuses `QueryPlanVisitor`'s leaf-relation matchers so dataset identity
 * stays consistent with the source/sink lineage that already ships.
 */
object ColumnLineageExtractor {

  /**
   * Extract a per-sink column-lineage facet for an analyzed plan.
   *
   * Returns an empty map when the plan has no sinks (e.g. a `df.collect()`
   * action), or when extraction fails for every sink.
   */
  def extract(plan: LogicalPlan): Map[(String, String), ColumnLineageFacet] = {
    try extractInternal(plan)
    catch { case NonFatal(_) => Map.empty }
  }

  private def extractInternal(plan: LogicalPlan): Map[(String, String), ColumnLineageFacet] = {
    val out = mutable.LinkedHashMap.empty[(String, String), ColumnLineageFacet]

    // We extract one facet per sink. `findSinks` returns a (DatasetRef, sinkPlan,
    // sinkColumnNames) triple per sink: the dataset identity, the LogicalPlan
    // whose output we should compute lineage for, and the canonical column
    // names of that sink (when available).
    findSinks(plan).foreach {
      case SinkBinding(dsRef, queryPlan, sinkColumnNames) =>
        val facet = try buildFacet(queryPlan, sinkColumnNames)
                    catch { case NonFatal(_) => ColumnLineageFacet.Empty }
        if (facet.nonEmpty) out.update(dsRef.identityKey, facet)
    }

    out.toMap
  }

  private def buildFacet(
      queryPlan: LogicalPlan,
      sinkColumnNames: Option[Seq[String]]
  ): ColumnLineageFacet = {
    val state = visit(queryPlan)
    val output = queryPlan.output

    // Resolve canonical sink column names. When the sink advertises explicit
    // column names (e.g. V2 table schema, V1 outputColumnNames), those win;
    // otherwise we fall back to the query's output attribute names.
    val names: Seq[String] = sinkColumnNames match {
      case Some(ns) if ns.length == output.length => ns
      case _                                       => output.map(_.name)
    }

    val fields: Map[String, Seq[ColumnLineageInputField]] =
      output.zip(names).flatMap { case (attr, name) =>
        state.attrLineage.get(attr.exprId) match {
          case Some(inputs) if inputs.nonEmpty => Some(name -> inputs)
          case _                                => None
        }
      }.toMap

    ColumnLineageFacet(fields = fields, dataset = state.indirect)
  }

  // --------------------------------------------------------------------------
  // Sink discovery
  // --------------------------------------------------------------------------

  private final case class SinkBinding(
      dsRef: DatasetRef,
      queryPlan: LogicalPlan,
      sinkColumnNames: Option[Seq[String]]
  )

  @nowarn("cat=deprecation")
  private def findSinks(plan: LogicalPlan): Seq[SinkBinding] = {
    val out = mutable.ArrayBuffer.empty[SinkBinding]

    plan.foreach {
      case c: V2WriteCommand =>
        val ref = QueryPlanVisitor.fromV2WriteCommand(c)
        val sinkCols = try Some(c.table.schema.fields.toIndexedSeq.map(_.name))
                       catch { case NonFatal(_) => None }
        out += SinkBinding(ref, c.query, sinkCols)

      case c: CreateTableAsSelect =>
        QueryPlanVisitor.fromV2CreateTableAsSelect(c).foreach { ref =>
          out += SinkBinding(ref, c.query, None)
        }

      case c: ReplaceTableAsSelect =>
        QueryPlanVisitor.fromV2ReplaceTableAsSelect(c).foreach { ref =>
          out += SinkBinding(ref, c.query, None)
        }

      case c: InsertIntoHadoopFsRelationCommand =>
        QueryPlanVisitor.fromInsertIntoHadoopFs(c).foreach { ref =>
          val sinkCols =
            if (c.outputColumnNames.nonEmpty) Some(c.outputColumnNames.toIndexedSeq) else None
          out += SinkBinding(ref, c.query, sinkCols)
        }

      case c: SaveIntoDataSourceCommand =>
        QueryPlanVisitor.fromSaveIntoDataSource(c).foreach { ref =>
          out += SinkBinding(ref, c.query, None)
        }

      case c: CreateDataSourceTableAsSelectCommand =>
        val ref = QueryPlanVisitor.fromCreateDataSourceTas(c)
        out += SinkBinding(ref, c.query, None)

      case c: WriteToDataSourceV2 =>
        QueryPlanVisitor.fromWriteToDataSourceV2(c).foreach { ref =>
          out += SinkBinding(ref, c.query, None)
        }

      case c: WriteToMicroBatchDataSource =>
        QueryPlanVisitor.fromWriteToMicroBatch(c).foreach { ref =>
          out += SinkBinding(ref, c.query, None)
        }

      case _ => ()
    }

    out.toSeq
  }

  // --------------------------------------------------------------------------
  // Plan walker
  // --------------------------------------------------------------------------

  /**
   * State produced by visiting a logical-plan subtree.
   *
   *   - `attrLineage` maps each `ExprId` produced anywhere in the subtree to
   *     the DIRECT input contributions for that attribute. Containing the
   *     entire subtree (not just the current node's outputs) lets parents
   *     resolve attribute references that flow through `Filter`/`Sort`/etc.
   *     unchanged.
   *   - `indirect` is the set of dataset-level INDIRECT dependencies seen
   *     in the subtree (filter/sort/group-by/join/window inputs).
   */
  private final case class VisitState(
      attrLineage: Map[ExprId, Seq[ColumnLineageInputField]],
      indirect: Seq[ColumnLineageInputField]
  )

  private object VisitState {
    val Empty: VisitState = VisitState(Map.empty, Seq.empty)
  }

  private def visit(plan: LogicalPlan): VisitState = plan match {
    case l: LogicalRelation         => leafState(QueryPlanVisitor.fromLogicalRelation(l), l.output)
    case h: HiveTableRelation       => leafState(Some(QueryPlanVisitor.fromHiveTableRelation(h)), h.output)
    case v: DataSourceV2Relation    => leafState(Some(QueryPlanVisitor.fromV2Relation(v)), v.output)
    case s: DataSourceV2ScanRelation => leafState(Some(QueryPlanVisitor.fromV2Relation(s.relation)), s.output)

    case p: Project =>
      val child = visit(p.child)
      val newAttrs = p.projectList.map { ne =>
        ne.exprId -> resolveExpressionInputs(ne, child.attrLineage)
      }
      VisitState(child.attrLineage ++ newAttrs, child.indirect)

    case a: Aggregate =>
      val child = visit(a.child)
      val newAttrs = a.aggregateExpressions.map { ne =>
        ne.exprId -> resolveExpressionInputs(ne, child.attrLineage)
      }
      val groupDeps = a.groupingExpressions.flatMap(e => referencedInputs(e, child.attrLineage))
      val groupIndirect = withTransformation(mergeAll(Seq(groupDeps)), GroupBy)
      VisitState(
        child.attrLineage ++ newAttrs,
        mergeFields(child.indirect, groupIndirect)
      )

    case f: Filter =>
      val child = visit(f.child)
      val deps  = referencedInputs(f.condition, child.attrLineage)
      val withT = withTransformation(deps, FilterTrans)
      VisitState(child.attrLineage, mergeFields(child.indirect, withT))

    case s: Sort =>
      val child = visit(s.child)
      val deps  = s.order.flatMap(o => referencedInputs(o.child, child.attrLineage))
      val withT = withTransformation(mergeAll(Seq(deps)), SortTrans)
      VisitState(child.attrLineage, mergeFields(child.indirect, withT))

    case j: Join =>
      val left  = visit(j.left)
      val right = visit(j.right)
      val combined = left.attrLineage ++ right.attrLineage
      val condDeps = j.condition.toSeq.flatMap(c => referencedInputs(c, combined))
      val withT    = withTransformation(mergeAll(Seq(condDeps)), JoinTrans)
      VisitState(
        combined,
        mergeFields(mergeFields(left.indirect, right.indirect), withT)
      )

    case w: Window =>
      val child = visit(w.child)
      val newAttrs = w.windowExpressions.map { ne =>
        ne.exprId -> resolveExpressionInputs(ne, child.attrLineage)
      }
      val partDeps  = w.partitionSpec.flatMap(e => referencedInputs(e, child.attrLineage))
      val orderDeps = w.orderSpec.flatMap(o => referencedInputs(o.child, child.attrLineage))
      val deps      = withTransformation(mergeAll(Seq(partDeps ++ orderDeps)), WindowTrans)
      VisitState(child.attrLineage ++ newAttrs, mergeFields(child.indirect, deps))

    case g: Generate =>
      val child = visit(g.child)
      val genInputs = referencedInputs(g.generator, child.attrLineage)
      val labelled = withTransformation(genInputs, transformation(prettyName(g.generator)))
      val genAttrs = g.generatorOutput.map(_.exprId -> labelled)
      VisitState(child.attrLineage ++ genAttrs.toMap, child.indirect)

    case s: SubqueryAlias =>
      val child = visit(s.child)
      val rebound = s.output.zip(s.child.output).flatMap { case (out, in) =>
        child.attrLineage.get(in.exprId).map(out.exprId -> _)
      }
      VisitState(child.attrLineage ++ rebound.toMap, child.indirect)

    case u: Union =>
      val children = u.children.map(visit)
      val combined = children.foldLeft(Map.empty[ExprId, Seq[ColumnLineageInputField]])(_ ++ _.attrLineage)
      val unionAttrs: Map[ExprId, Seq[ColumnLineageInputField]] = if (u.children.nonEmpty) {
        u.output.zipWithIndex.map { case (outAttr, idx) =>
          val perChild = u.children.zip(children).flatMap { case (childPlan, childState) =>
            if (idx < childPlan.output.length) {
              childState.attrLineage.get(childPlan.output(idx).exprId)
            } else None
          }
          outAttr.exprId -> mergeAll(perChild)
        }.toMap
      } else Map.empty
      val indirect = mergeAll(children.map(_.indirect))
      VisitState(combined ++ unionAttrs, indirect)

    case other =>
      // Unknown plan node: union children, surface their outputs through this
      // node's own output attributes by position when shapes match. This keeps
      // pass-through nodes (Limit, Distinct, Repartition, Coalesce, Sample,
      // GlobalLimit, LocalLimit, Deduplicate, ...) lineage-preserving without
      // having to enumerate them.
      val children = other.children.map(visit)
      val combined = children.foldLeft(Map.empty[ExprId, Seq[ColumnLineageInputField]])(_ ++ _.attrLineage)
      val passthrough: Map[ExprId, Seq[ColumnLineageInputField]] =
        if (other.children.size == 1 && other.children.head.output.length == other.output.length) {
          other.output.zip(other.children.head.output).flatMap { case (out, in) =>
            combined.get(in.exprId).map(out.exprId -> _)
          }.toMap
        } else Map.empty
      val indirect = mergeAll(children.map(_.indirect))
      VisitState(combined ++ passthrough, indirect)
  }

  private def leafState(
      ref: Option[DatasetRef],
      output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute]
  ): VisitState = ref match {
    case None => VisitState.Empty
    case Some(ds) =>
      val seeded = output.collect { case a: AttributeReference =>
        a.exprId -> Seq(ColumnLineageInputField(ds.namespace, ds.name, a.name))
      }.toMap
      VisitState(seeded, Seq.empty)
  }

  // --------------------------------------------------------------------------
  // Expression resolution
  // --------------------------------------------------------------------------

  /**
   * Resolve the DIRECT input contributions for a single output-bearing
   * expression (a `NamedExpression` from a `Project`, `Aggregate`, or
   * `Window`). Walks the expression tree, collects referenced
   * `AttributeReference`s, looks them up in the child's lineage map, and
   * tags them with the appropriate per-output transformation derived from
   * the expression shape.
   *
   * The classifier returns `Identity` for pure pass-throughs and the
   * appropriate `TRANSFORMATION` / `AGGREGATION` entry otherwise; we always
   * attach it so callers can see how each input was consumed (the
   * transformations list dedups identical entries upstream, so chained
   * projections don't accumulate redundant `IDENTITY` markers).
   */
  private def resolveExpressionInputs(
      expr: NamedExpression,
      childLineage: Map[ExprId, Seq[ColumnLineageInputField]]
  ): Seq[ColumnLineageInputField] = {
    val unwrapped     = unwrapAlias(expr)
    val transformation = classify(unwrapped)
    val baseInputs    = referencedInputs(unwrapped, childLineage)
    if (baseInputs.isEmpty) baseInputs
    else withTransformation(baseInputs, transformation)
  }

  /**
   * Find every `AttributeReference` in `expr` whose `exprId` is known to
   * `childLineage`, and return their merged input fields. Inputs are
   * deduplicated by `(namespace, name, field)` so a column referenced twice
   * (e.g. `x + x`) doesn't show up twice.
   */
  private def referencedInputs(
      expr: Expression,
      childLineage: Map[ExprId, Seq[ColumnLineageInputField]]
  ): Seq[ColumnLineageInputField] = {
    val collected = expr.references.toSeq.flatMap { ar =>
      childLineage.get(ar.exprId).getOrElse(Seq.empty)
    }
    mergeAll(Seq(collected))
  }

  // --------------------------------------------------------------------------
  // Transformation classification
  // --------------------------------------------------------------------------

  /**
   * Classify a single output expression and produce the transformation
   * entry to attach to each input field. Pure pass-throughs return
   * `Identity`; aggregate expressions return `AGGREGATION` with the
   * aggregate function's pretty name; everything else is `TRANSFORMATION`.
   */
  private def classify(expr: Expression): ColumnLineageTransformation =
    expr match {
      case _: AttributeReference =>
        Identity

      case ae: AggregateExpression =>
        aggregation(prettyName(ae.aggregateFunction))

      case e if containsAggregate(e) =>
        aggregation(prettyName(e))

      case e =>
        val name    = prettyName(e)
        val masking = isMaskingExpression(e)
        transformation(name, masking = masking)
    }

  private def unwrapAlias(expr: Expression): Expression = expr match {
    case a: Alias => unwrapAlias(a.child)
    case other    => other
  }

  private def containsAggregate(expr: Expression): Boolean = expr.exists {
    case _: AggregateExpression => true
    case _                       => false
  }

  /**
   * Heuristic for "this transformation hides the input". Hash families and
   * the explicit `mask` builtin qualify; we keep this conservative because a
   * false positive here (claiming masking when it isn't) is worse than a
   * false negative.
   */
  private def isMaskingExpression(expr: Expression): Boolean = {
    val name = prettyName(expr).toLowerCase
    MaskingFunctionNames.contains(name) || expr.exists {
      case e if MaskingFunctionNames.contains(prettyName(e).toLowerCase) => true
      case _ => false
    }
  }

  private val MaskingFunctionNames: Set[String] = Set(
    "md5",
    "sha",
    "sha1",
    "sha2",
    "hash",
    "xxhash64",
    "mask",
    "mask_show_first_n",
    "mask_show_last_n",
    "mask_first_n",
    "mask_last_n",
    "mask_hash"
  )

  /**
   * Best-effort label for an expression — `Expression.prettyName` is what
   * SQL would render (e.g. `sum`, `md5`, `regexp_replace`); for unusual
   * subclasses we fall back to the simple class name.
   */
  private def prettyName(expr: Expression): String = {
    val tryPretty =
      try expr.prettyName
      catch { case NonFatal(_) => "" }
    if (tryPretty != null && tryPretty.nonEmpty) tryPretty
    else stripScalaObjectSuffix(expr.getClass.getSimpleName)
  }

  private def stripScalaObjectSuffix(name: String): String =
    if (name.endsWith("$")) name.dropRight(1) else name
}
