package com.openlakehouse.lineage

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}

import com.openlakehouse.lineage.driver.LineageDriverPlugin
import com.openlakehouse.lineage.executor.LineageExecutorPlugin

/**
 * Entry point for the OpenLineage Spark plugin.
 *
 * Register on the driver (classic mode) via:
 *   --conf spark.plugins=com.openlakehouse.lineage.LineagePlugin
 *
 * In Spark Connect mode this same config must be set on the
 * spark-connect-server JVM, not on the Connect client.
 */
final class LineagePlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin     = new LineageDriverPlugin()
  override def executorPlugin(): ExecutorPlugin = new LineageExecutorPlugin()
}
