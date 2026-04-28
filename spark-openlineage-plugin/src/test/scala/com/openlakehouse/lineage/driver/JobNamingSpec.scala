package com.openlakehouse.lineage.driver

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.openlakehouse.lineage.LineageConfig

final class JobNamingSpec extends AnyFunSuite with Matchers {

  private val emptyPlan: LogicalPlan =
    LocalRelation(AttributeReference("x", IntegerType)() :: Nil)

  private def configWithOverride(name: Option[String]): LineageConfig = LineageConfig(
    serviceUrl              = None,
    namespace               = "prod",
    jobNameOverride         = name,
    emitTaskMetrics         = false,
    emitColumnLineage       = false,
    streamingProgressEveryN = 1,
    queueSize               = 1,
    batchFlushMs            = 1L,
    disabled                = false,
    failOpen                = true
  )

  test("explicit jobNameOverride wins over description and app name") {
    val cfg = configWithOverride(Some("my-etl-job"))
    val conf = new SparkConf().set("spark.app.name", "irrelevant-app")
    val ref  = JobNaming.derive(cfg, conf, emptyPlan, description = Some("also-irrelevant"))
    ref.name shouldBe "my-etl-job"
    ref.namespace shouldBe "prod"
  }

  test("description is used when no override is set") {
    val cfg  = configWithOverride(None)
    val conf = new SparkConf().set("spark.app.name", "my-app")
    val ref  = JobNaming.derive(cfg, conf, emptyPlan, description = Some("load-daily-users"))
    ref.name shouldBe "load-daily-users"
  }

  test("falls back to appName.plan_<hash> when no override/description") {
    val cfg  = configWithOverride(None)
    val conf = new SparkConf().set("spark.app.name", "my-app")
    val ref  = JobNaming.derive(cfg, conf, emptyPlan)
    ref.name should startWith("my-app.plan_")
    ref.name.stripPrefix("my-app.plan_").length shouldBe 10
  }

  test("derived name is stable for the same plan (deterministic hash)") {
    val cfg  = configWithOverride(None)
    val conf = new SparkConf().set("spark.app.name", "app")
    val a    = JobNaming.derive(cfg, conf, emptyPlan).name
    val b    = JobNaming.derive(cfg, conf, emptyPlan).name
    a shouldBe b
  }

  test("sanitise lowercases, replaces unsafe chars, collapses underscores, trims") {
    JobNaming.sanitise("Hello World!")     shouldBe "hello_world"
    JobNaming.sanitise("my__job..name--")  shouldBe "my_job..name--"
    JobNaming.sanitise("___trim___")        shouldBe "trim"
    JobNaming.sanitise("keep.dots.and-dash") shouldBe "keep.dots.and-dash"
  }

  test("sanitiseNamespace preserves slashes (catalog paths)") {
    JobNaming.sanitiseNamespace("warehouse/prod/db") shouldBe "warehouse/prod/db"
    JobNaming.sanitiseNamespace("MY NS!/path")       shouldBe "my_ns_/path"
  }

  test("namespace with unsafe chars is sanitised on the way out") {
    val cfg  = configWithOverride(Some("n")).copy(namespace = "Prod Env!")
    val conf = new SparkConf().set("spark.app.name", "a")
    JobNaming.derive(cfg, conf, emptyPlan).namespace shouldBe "prod_env"
  }
}
