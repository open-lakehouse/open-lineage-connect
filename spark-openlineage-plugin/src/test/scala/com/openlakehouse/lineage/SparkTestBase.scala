package com.openlakehouse.lineage

import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.stream.Stream

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Mixin that provides a real in-process SparkSession scoped to one test suite.
 *
 * We spin up a `local[1]` session because `QueryPlanVisitor` needs real
 * Catalyst `LogicalPlan` instances — constructing those synthetically pulls in
 * too many private Spark internals to be worth it. `fork := true` in build.sbt
 * means each test class gets a fresh JVM with the required `--add-opens` flags.
 */
trait SparkTestBase extends BeforeAndAfterAll { self: Suite =>

  protected var spark: SparkSession = _
  protected var tmpDir: Path        = _

  protected def extraSparkConf: SparkConf = new SparkConf()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    tmpDir = Files.createTempDirectory(s"openlineage-${getClass.getSimpleName}-")

    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local[1]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.warehouse.dir", tmpDir.resolve("warehouse").toString)
      .set("spark.driver.host", "localhost")
      .set("spark.driver.bindAddress", "127.0.0.1")

    // Allow subclasses to layer on test-specific settings (e.g. Delta extensions).
    extraSparkConf.getAll.foreach { case (k, v) => conf.set(k, v) }

    spark = SparkSession.builder().config(conf).getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      deleteRecursively(tmpDir)
      super.afterAll()
    }
  }

  private def deleteRecursively(root: Path): Unit = {
    if (root != null && Files.exists(root)) {
      val stream: Stream[Path] = Files.walk(root)
      try {
        stream.sorted(Comparator.reverseOrder[Path]()).forEach { p =>
          try { Files.deleteIfExists(p); () } catch { case _: Throwable => () }
        }
      } finally stream.close()
    }
  }
}
