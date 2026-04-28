package com.openlakehouse.lineage.driver

import java.lang.reflect.Method

import scala.util.Try

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Connector-specific write commands that aren't on the core Spark classpath.
 *
 * In Spark 4.x the big three (Delta, Iceberg, Hudi) lower writes to
 * `V2WriteCommand`, so `QueryPlanVisitor.extractSinks` already catches them.
 * But some connectors still materialise their own internal command nodes
 * (e.g. Delta's `WriteIntoDelta` for pre-v2 code paths, Hive's
 * `CreateHiveTableAsSelectCommand`). We handle those with a light reflective
 * pass: if the class loads, we try a few well-known accessors to recover
 * a dataset identity; if not, we skip.
 *
 * This keeps the plugin dependency-free of those connectors while still
 * producing useful lineage when they happen to be on the user's classpath.
 *
 * All reflection is wrapped in `Try` — any failure returns `None` for that
 * matcher and leaves the other matchers untouched.
 */
private[driver] object ReflectiveSinkMatchers {

  /** A single connector-specific matcher. */
  private final case class Matcher(
      /** Fully-qualified class name we expect on the plan. */
      className: String,
      /** Short label recorded as `sourceType`. */
      sourceType: String,
      /** Extracts (namespace, name) from an instance of `className`. */
      extract: AnyRef => Option[(String, String)]
  )

  private val matchers: Seq[Matcher] = Seq(
    // Delta: legacy WriteIntoDelta command. New Delta releases use v2 writes.
    Matcher(
      className  = "org.apache.spark.sql.delta.commands.WriteIntoDelta",
      sourceType = "delta",
      extract    = cmd => getLocation(cmd, "deltaLog", "dataPath").map(p => ("delta", p))
    ),
    // Delta MERGE INTO (as of delta-spark 4.x often materialises as its own node).
    Matcher(
      className  = "org.apache.spark.sql.delta.commands.MergeIntoCommand",
      sourceType = "delta",
      extract    = cmd => getLocation(cmd, "target", "dataPath")
                           .orElse(getLocation(cmd, "target", "tableIdentifier", "table"))
                           .map(p => ("delta", p))
    ),
    // Hive CTAS: lives in spark-hive, not spark-sql.
    Matcher(
      className  = "org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand",
      sourceType = "hive",
      extract    = cmd => catalogTableIdentity(cmd, "tableDesc").orElse(catalogTableIdentity(cmd, "table"))
    ),
    // Iceberg internal write node — typically only seen pre-v2 or for certain
    // operations like `DELETE FROM`/`UPDATE`.
    Matcher(
      className  = "org.apache.iceberg.spark.source.SparkWrite$CommitCommand",
      sourceType = "iceberg",
      extract    = cmd => stringAccessor(cmd, "tableName").map(n => ("iceberg", n))
    )
  )

  /**
   * Returns any dataset refs surfaced by connector-specific commands on `plan`.
   * Safe to call on any plan — unknown commands are silently skipped.
   */
  def matchAll(plan: LogicalPlan): Seq[DatasetRef] = {
    val loader = Thread.currentThread().getContextClassLoader
    val activeMatchers = matchers.flatMap { m =>
      Try(Class.forName(m.className, false, loader)).toOption.map(cls => (cls, m))
    }
    if (activeMatchers.isEmpty) return Seq.empty

    plan.collect {
      case node if activeMatchers.exists { case (cls, _) => cls.isInstance(node) } =>
        activeMatchers.collectFirst {
          case (cls, m) if cls.isInstance(node) =>
            m.extract(node).map { case (ns, name) =>
              DatasetRef(namespace = ns, name = name, sourceType = m.sourceType, facets = Map("writeKind" -> m.className))
            }
        }.flatten
    }.flatten
  }

  // --------------------------------------------------------------------------
  // Reflection helpers
  // --------------------------------------------------------------------------

  /**
   * Walk a chain of zero-arg accessors (methods or public fields), calling each
   * one on the result of the previous. Returns the final value as a String.
   */
  private def getLocation(target: AnyRef, path: String*): Option[String] = {
    val finalVal = path.foldLeft(Option(target: AnyRef)) { (acc, step) =>
      acc.flatMap(obj => invokeZeroArg(obj, step))
    }
    finalVal.map(_.toString).filter(_.nonEmpty)
  }

  private def stringAccessor(target: AnyRef, method: String): Option[String] =
    invokeZeroArg(target, method).map(_.toString).filter(_.nonEmpty)

  private def invokeZeroArg(target: AnyRef, name: String): Option[AnyRef] = Try {
    val cls: Class[_] = target.getClass
    val m: Method     = findZeroArgMethod(cls, name)
    m.setAccessible(true)
    m.invoke(target)
  }.toOption.flatMap(Option(_))

  private def findZeroArgMethod(cls: Class[_], name: String): Method = {
    // Search up the class hierarchy.
    var current: Class[_] = cls
    while (current != null) {
      current.getDeclaredMethods.find(m => m.getName == name && m.getParameterCount == 0) match {
        case Some(m) => return m
        case None    => current = current.getSuperclass
      }
    }
    // Also check interfaces (for default methods on Scala traits).
    cls.getMethod(name)
  }

  /**
   * Extract (database, table) from a `CatalogTable` field on a command.
   */
  private def catalogTableIdentity(cmd: AnyRef, field: String): Option[(String, String)] = {
    for {
      ct   <- invokeZeroArg(cmd, field)
      id   <- invokeZeroArg(ct, "identifier")
      table = stringAccessor(id, "table").getOrElse("unknown")
      db    = invokeZeroArg(id, "database").flatMap(d => invokeZeroArg(d, "getOrElse").map(_.toString))
    } yield (db.getOrElse("default"), table)
  }
}
