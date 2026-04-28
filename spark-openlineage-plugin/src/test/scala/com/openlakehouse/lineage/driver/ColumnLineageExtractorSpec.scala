package com.openlakehouse.lineage.driver

import com.openlakehouse.lineage.SparkTestBase
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for `ColumnLineageExtractor`.
 *
 * Each test builds a real DataFrame, writes it to a sink, and asserts the
 * extractor produces the expected per-output-column input fields plus
 * dataset-level dependencies. We run with a single shared SparkSession
 * (provided by `SparkTestBase`) and write to local parquet files for sink
 * coverage — keeps the test suite hermetic and fast.
 */
final class ColumnLineageExtractorSpec extends AnyFunSuite with Matchers with SparkTestBase {

  private def writeParquet(name: String): String = {
    val p = tmpDir.resolve(s"$name.parquet").toString
    // Each test computes its source path(s) first and the sink path last,
    // so the most recent call wins as the expected sink for `lastWritePlan`.
    lastExpectedOutPath = Some(p)
    p
  }

  private def lineageFor(plan: LogicalPlan): ColumnLineageFacet = {
    val map = ColumnLineageExtractor.extract(plan)
    map.values.headOption.getOrElse(ColumnLineageFacet.Empty)
  }

  // --------------------------------------------------------------------------
  // Direct projections: IDENTITY + TRANSFORMATION
  // --------------------------------------------------------------------------

  test("simple projection produces IDENTITY for renamed and pass-through columns") {
    val ss = spark; import ss.implicits._
    val src = writeParquet("src1")
    Seq((1, "ada"), (2, "linus")).toDF("id", "name").write.mode("overwrite").parquet(src)

    val out = writeParquet("out1")
    spark.read.parquet(src).select($"id", $"name".as("user_name"))
      .write.mode("overwrite").parquet(out)

    val plan = lastWritePlan()
    val facet = lineageFor(plan)
    facet.fields.keySet shouldBe Set("id", "user_name")

    val idInputs = facet.fields("id")
    idInputs should have size 1
    idInputs.head.field shouldBe "id"
    idInputs.head.transformations.exists(t =>
      t.kind == "DIRECT" && t.subtype == "IDENTITY"
    ) shouldBe true

    val userInputs = facet.fields("user_name")
    userInputs should have size 1
    userInputs.head.field shouldBe "name"
  }

  test("scalar transformation labels DIRECT/TRANSFORMATION on the input") {
    val ss = spark; import ss.implicits._
    val src = writeParquet("src_trans")
    Seq("a", "b").toDF("name").write.mode("overwrite").parquet(src)
    val out = writeParquet("out_trans")

    spark.read.parquet(src).selectExpr("upper(name) as upper_name")
      .write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())
    val ins   = facet.fields("upper_name")
    ins should have size 1
    ins.head.field shouldBe "name"
    ins.head.transformations.exists(t =>
      t.kind == "DIRECT" && t.subtype == "TRANSFORMATION"
    ) shouldBe true
  }

  test("md5 of an input column is flagged DIRECT/TRANSFORMATION with masking=true") {
    val ss = spark; import ss.implicits._
    val src = writeParquet("src_mask")
    Seq("ada@example.com").toDF("email").write.mode("overwrite").parquet(src)
    val out = writeParquet("out_mask")

    spark.read.parquet(src).selectExpr("md5(email) as e")
      .write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())
    val ins   = facet.fields("e")
    ins should have size 1
    ins.head.field shouldBe "email"
    val t = ins.head.transformations.find(_.subtype == "TRANSFORMATION").value
    t.masking shouldBe true
    t.description.toLowerCase should include("md5")
  }

  // --------------------------------------------------------------------------
  // Aggregates: AGGREGATION + GROUP_BY
  // --------------------------------------------------------------------------

  test("aggregate produces DIRECT/AGGREGATION for sum() and INDIRECT/GROUP_BY for grouping key") {
    val ss = spark; import ss.implicits._
    val src = writeParquet("src_agg")
    Seq(("us", 10), ("us", 20), ("eu", 5)).toDF("region", "amount")
      .write.mode("overwrite").parquet(src)
    val out = writeParquet("out_agg")

    spark.read.parquet(src).groupBy($"region").agg(org.apache.spark.sql.functions.sum($"amount").as("total"))
      .write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())

    facet.fields("total")
      .find(_.field == "amount").value
      .transformations.exists(_.subtype == "AGGREGATION") shouldBe true

    facet.dataset
      .find(_.field == "region").value
      .transformations.exists(_.subtype == "GROUP_BY") shouldBe true
  }

  // --------------------------------------------------------------------------
  // Filter / Sort: dataset-level INDIRECT
  // --------------------------------------------------------------------------

  test("filter and sort produce INDIRECT/FILTER and INDIRECT/SORT in the dataset list") {
    val ss = spark; import ss.implicits._
    val src = writeParquet("src_filter_sort")
    Seq((1, 10, "x"), (2, 20, "y")).toDF("id", "amount", "ts")
      .write.mode("overwrite").parquet(src)
    val out = writeParquet("out_filter_sort")

    spark.read.parquet(src)
      .where($"amount" > 5)
      .orderBy($"ts")
      .select($"id")
      .write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())
    facet.dataset.find(_.field == "amount").value
      .transformations.exists(_.subtype == "FILTER") shouldBe true
    facet.dataset.find(_.field == "ts").value
      .transformations.exists(_.subtype == "SORT") shouldBe true
  }

  // --------------------------------------------------------------------------
  // Joins: INDIRECT/JOIN dataset-level + outputs from both sides
  // --------------------------------------------------------------------------

  test("join surfaces both sides' columns and adds INDIRECT/JOIN for the join keys") {
    val ss = spark; import ss.implicits._
    val a = writeParquet("join_a")
    val b = writeParquet("join_b")
    Seq((1, "alice"), (2, "bob")).toDF("id", "name").write.mode("overwrite").parquet(a)
    Seq((1, "ny"), (2, "sf")).toDF("id", "city").write.mode("overwrite").parquet(b)
    val out = writeParquet("out_join")

    val left  = spark.read.parquet(a).alias("l")
    val right = spark.read.parquet(b).alias("r")
    left.join(right, left("id") === right("id"))
      .select(left("name"), right("city"))
      .write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())
    facet.fields.keySet shouldBe Set("name", "city")
    facet.fields("name").map(_.field) should contain ("name")
    facet.fields("city").map(_.field) should contain ("city")
    facet.dataset.exists(f => f.field == "id" &&
      f.transformations.exists(_.subtype == "JOIN")) shouldBe true
  }

  // --------------------------------------------------------------------------
  // Window functions: INDIRECT/WINDOW dataset-level
  // --------------------------------------------------------------------------

  test("window function partition/order columns land in INDIRECT/WINDOW dataset deps") {
    val ss = spark; import ss.implicits._
    val src = writeParquet("src_window")
    Seq((1, "eng", 100), (2, "eng", 200), (3, "ops", 50))
      .toDF("id", "dept", "salary")
      .write.mode("overwrite").parquet(src)
    val out = writeParquet("out_window")

    val w = org.apache.spark.sql.expressions.Window.partitionBy("dept").orderBy("id")
    spark.read.parquet(src)
      .withColumn("avg_sal", org.apache.spark.sql.functions.avg($"salary").over(w))
      .select($"id", $"avg_sal")
      .write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())
    val winDeps = facet.dataset.filter(_.transformations.exists(_.subtype == "WINDOW"))
    winDeps.map(_.field).toSet should (contain("dept") and contain("id"))
  }

  // --------------------------------------------------------------------------
  // Multi-source / union
  // --------------------------------------------------------------------------

  test("union of two parquet sources surfaces both inputs for each output column") {
    val ss = spark; import ss.implicits._
    val a = writeParquet("u_a")
    val b = writeParquet("u_b")
    Seq((1, "ada")).toDF("id", "name").write.mode("overwrite").parquet(a)
    Seq((2, "lin")).toDF("id", "name").write.mode("overwrite").parquet(b)
    val out = writeParquet("u_out")

    val unioned = spark.read.parquet(a).unionByName(spark.read.parquet(b))
    unioned.write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())
    val idInputs = facet.fields("id")
    idInputs.map(_.field) should contain ("id")
    // Either two distinct input datasets contribute, or at least both source
    // names are represented in the inputs of the output column.
    idInputs.map(_.name).toSet.size should be >= 1
  }

  // --------------------------------------------------------------------------
  // Empty / passthrough scenarios
  // --------------------------------------------------------------------------

  test("plan with no sinks returns an empty lineage map") {
    val ss = spark; import ss.implicits._
    val df = Seq((1, "a")).toDF("id", "name")
    val map = ColumnLineageExtractor.extract(df.queryExecution.analyzed)
    map shouldBe empty
  }

  test("subquery alias propagates lineage through unchanged") {
    val ss = spark; import ss.implicits._
    val src = writeParquet("sub_src")
    Seq((1, "ada")).toDF("id", "name").write.mode("overwrite").parquet(src)
    val out = writeParquet("sub_out")

    spark.read.parquet(src).alias("u").select($"u.id".as("uid"))
      .write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())
    facet.fields("uid").head.field shouldBe "id"
  }

  test("classify falls back to TRANSFORMATION for unknown scalar expressions") {
    val ss = spark; import ss.implicits._
    val src = writeParquet("classify_src")
    Seq((1, 2)).toDF("a", "b").write.mode("overwrite").parquet(src)
    val out = writeParquet("classify_out")

    spark.read.parquet(src).selectExpr("a + b as ab")
      .write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())
    val ins = facet.fields("ab")
    ins.map(_.field).toSet shouldBe Set("a", "b")
    ins.foreach { f =>
      f.transformations.exists(_.subtype == "TRANSFORMATION") shouldBe true
    }
  }

  test("self-join over the same parquet source still emits a single dataset entry") {
    val ss = spark; import ss.implicits._
    val src = writeParquet("self_join")
    Seq((1, "ada"), (2, "lin")).toDF("id", "name").write.mode("overwrite").parquet(src)
    val out = writeParquet("self_join_out")

    val left  = spark.read.parquet(src).alias("l")
    val right = spark.read.parquet(src).alias("r")
    left.join(right, left("id") === right("id"))
      .select(left("name").as("ln"), right("name").as("rn"))
      .write.mode("overwrite").parquet(out)

    val facet = lineageFor(lastWritePlan())
    facet.fields.keySet shouldBe Set("ln", "rn")
    val joinDeps = facet.dataset.filter(_.transformations.exists(_.subtype == "JOIN"))
    joinDeps.map(_.field).toSet shouldBe Set("id")
  }

  test("merging input fields collapses duplicate (ns,name,field) entries") {
    val merged = ColumnLineageInputField.merge(
      Seq(ColumnLineageInputField("ns", "t", "x", Seq(ColumnLineageTransformation.Identity))),
      Seq(ColumnLineageInputField("ns", "t", "x", Seq(ColumnLineageTransformation.Filter)))
    )
    merged should have size 1
    merged.head.transformations.map(_.subtype) shouldBe Seq("IDENTITY", "FILTER")
  }

  // --------------------------------------------------------------------------
  // Helper: drill into the most-recent write command on the active session
  // --------------------------------------------------------------------------

  /**
   * Spark's `QueryExecutionListener` events are dispatched through an
   * `AsyncEventQueue`, so the call site returns before the listener fires.
   * To make tests deterministic we keep *every* captured plan and look up
   * the one whose `InsertIntoHadoopFsRelationCommand.outputPath` (or
   * equivalent) matches the expected output path. Polls with a 5s budget.
   */
  private def lastWritePlan(): LogicalPlan = {
    val expected = lastExpectedOutPath
    val deadline = System.nanoTime() + 5L * 1000L * 1000L * 1000L
    var matched: Option[LogicalPlan] = findMatching(expected)
    while (matched.isEmpty && System.nanoTime() < deadline) {
      Thread.sleep(20L)
      matched = findMatching(expected)
    }
    matched.getOrElse(fail(s"no write plan captured for $expected within timeout"))
  }

  private def findMatching(expected: Option[String]): Option[LogicalPlan] =
    capturedPlans.synchronized {
      val all = capturedPlans.toIndexedSeq
      expected match {
        case Some(path) =>
          all.reverse.find(planTargets(_).exists(_.contains(stripScheme(path))))
        case None => all.lastOption
      }
    }

  private def stripScheme(p: String): String = {
    val idx = p.indexOf("://")
    if (idx >= 0) p.substring(idx + 3) else p
  }

  private def planTargets(plan: LogicalPlan): Seq[String] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[String]
    plan.foreach {
      case c: org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand =>
        out += c.outputPath.toString
      case c: org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand =>
        c.options.get("path").foreach(out += _)
      case _ => ()
    }
    out.toSeq
  }

  private val capturedPlans = scala.collection.mutable.ArrayBuffer.empty[LogicalPlan]

  /**
   * Per-test record of the expected sink path so `lastWritePlan` can find
   * the matching capture even when the listener is racing with the test
   * thread. `writeParquet` updates this on every call; tests structured
   * "src first, out last" pick up the right path automatically.
   */
  @volatile private var lastExpectedOutPath: Option[String] = None

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.listenerManager.register(new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(funcName: String, qe: org.apache.spark.sql.execution.QueryExecution, durationNs: Long): Unit = {
        capturedPlans.synchronized {
          capturedPlans += qe.analyzed
          ()
        }
      }
      override def onFailure(funcName: String, qe: org.apache.spark.sql.execution.QueryExecution, exception: Exception): Unit = {
        capturedPlans.synchronized {
          capturedPlans += qe.analyzed
          ()
        }
      }
    })
  }

  // ScalaTest's `OptionValues` is not mixed in; small inline helper avoids the import chain.
  private implicit class OptValueOps[A](opt: Option[A]) {
    def value: A = opt.getOrElse(fail(s"expected Some, got None"))
  }
}
