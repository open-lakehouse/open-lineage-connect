package com.openlakehouse.lineage.driver

import java.security.MessageDigest

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.openlakehouse.lineage.LineageConfig

/**
 * Job-naming policy. A Spark query needs a *stable* lineage name that downstream
 * consumers can use to stitch runs of "the same pipeline" together.
 *
 * Precedence (first match wins):
 *   1. Explicit override: `spark.openlineage.jobName=<name>`.
 *   2. Description hint from the `SparkListenerSQLExecutionStart` event
 *      (when available) — this is the `QueryExecution.toString` or a caller-
 *      supplied SQL description.
 *   3. Computed: `<appName>.plan_<short-sha1(analyzed-plan-canonical)>`.
 *
 * Namespace comes from config (`spark.openlineage.namespace`, default `default`).
 */
object JobNaming {

  /** Maximum length for names derived from SQL descriptions. */
  private val MaxDescriptionLength = 96
  /** Length of the short hash appended to auto-generated names. */
  private val ShortHashLength      = 10

  def derive(
      config: LineageConfig,
      sparkConf: SparkConf,
      plan: LogicalPlan,
      description: Option[String] = None
  ): JobRef = {
    val namespace = config.namespace
    val appName   = Option(sparkConf.get("spark.app.name", "spark-app")).getOrElse("spark-app")

    val rawName = config.jobNameOverride
      .orElse(description.map(sanitise).filter(_.nonEmpty).map(truncate(_, MaxDescriptionLength)))
      .getOrElse(s"$appName.plan_${planShortHash(plan)}")

    JobRef(namespace = sanitiseNamespace(namespace), name = sanitise(rawName))
  }

  /**
   * A short SHA-1 prefix of a plan's canonicalized string form. Canonicalization
   * is Spark's `canonicalized.toString` where available; we fall back to
   * `plan.toString` when canonicalization fails on unresolved plans.
   */
  def planShortHash(plan: LogicalPlan): String = {
    val canonical =
      try plan.canonicalized.toString
      catch { case _: Throwable => plan.toString }
    sha1Hex(canonical).take(ShortHashLength)
  }

  private def sha1Hex(s: String): String = {
    val md = MessageDigest.getInstance("SHA-1")
    md.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  /**
   * Lowercase, replace anything outside `[a-z0-9_.-]` with `_`, collapse
   * consecutive `_`, trim leading/trailing `_`.
   *
   * Scoped deliberately permissive: OpenLineage doesn't mandate a specific
   * charset for names, but downstream viewers (Marquez, Datahub) expect
   * filesystem-safe identifiers.
   */
  def sanitise(s: String): String = {
    val lower    = s.toLowerCase
    val replaced = lower.map(c => if (isSafe(c)) c else '_').mkString
    val collapsed = "_+".r.replaceAllIn(replaced, "_")
    collapsed.stripPrefix("_").stripSuffix("_")
  }

  /** Namespaces allow `/` as a separator (Iceberg/Glue catalogs); otherwise same rules as names. */
  def sanitiseNamespace(s: String): String = {
    val lower    = s.toLowerCase
    val replaced = lower.map(c => if (isSafe(c) || c == '/') c else '_').mkString
    "_+".r.replaceAllIn(replaced, "_").stripPrefix("_").stripSuffix("_")
  }

  private def isSafe(c: Char): Boolean =
    (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-'

  private def truncate(s: String, max: Int): String =
    if (s.length <= max) s else s.substring(0, max)
}
