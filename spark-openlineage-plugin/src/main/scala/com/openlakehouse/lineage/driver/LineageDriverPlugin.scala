package com.openlakehouse.lineage.driver

import java.util.{Collections, HashMap => JHashMap, Map => JMap}

import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import com.openlakehouse.lineage.LineageConfig
import com.openlakehouse.lineage.common.ExecutorTaskMetrics
import com.openlakehouse.lineage.streaming.LineageStreamingListener
import com.openlakehouse.lineage.transport.{ConnectRpcClient, ConnectRpcEventSink, LineageServiceClient}

/**
 * Driver half of the lineage plugin.
 *
 * Responsibilities:
 *   - Register listeners: QueryExecutionListener (done), SparkListener (TBD),
 *     StreamingQueryListener (TBD).
 *   - Receive TaskMetricsPayload messages from executors via Spark plugin RPC (TBD).
 *   - Build and emit RunEvent / JobEvent / DatasetEvent protobufs to
 *     open-lineage-service over ConnectRPC (TBD, currently a Noop sink).
 *
 * When `spark.openlineage.disabled=true` the plugin short-circuits and registers
 * nothing. When `spark.openlineage.serviceUrl` is unset the plugin still registers
 * its listeners but routes every event to `EventSink.Noop` — this lets operators
 * flip the plugin on/off with a single config without bouncing the driver.
 */
final class LineageDriverPlugin extends DriverPlugin with Logging {

  @volatile private var config: LineageConfig               = _
  @volatile private var sink: EventSink                     = EventSink.Noop
  @volatile private var listener: LineageQueryListener      = _
  @volatile private var streamingListener: LineageStreamingListener = _
  private val metricsAggregator: TaskMetricsAggregator       = new TaskMetricsAggregator

  override def init(sc: SparkContext, pluginContext: PluginContext): JMap[String, String] = {
    try {
      config = LineageConfig.fromSparkConf(sc.getConf)

      if (config.disabled) {
        logInfo("OpenLineage plugin is disabled via spark.openlineage.disabled=true")
        return Collections.emptyMap[String, String]()
      }

      // Pick an event sink based on config. Real ConnectRPC emitter wiring is
      // a follow-up todo; for now we keep the plugin wiring end-to-end testable
      // via the Noop sink when no service URL is configured.
      sink = buildSink(config)

      if (config.serviceUrl.isEmpty) {
        logWarning(
          "OpenLineage plugin loaded but spark.openlineage.serviceUrl is unset — " +
            "events will be routed to the Noop sink"
        )
      } else {
        logInfo(s"OpenLineage plugin initialized (namespace=${config.namespace}, " +
          s"serviceUrl=${config.serviceUrl.get}, failOpen=${config.failOpen})")
      }

      registerQueryListener(sc)

      val extraConf = new JHashMap[String, String]()
      extraConf.put(LineageConfig.NamespaceKey, config.namespace)
      extraConf.put(LineageConfig.EmitTaskMetricsKey, config.emitTaskMetrics.toString)
      extraConf.put(LineageConfig.DisabledKey, config.disabled.toString)
      extraConf
    } catch {
      case NonFatal(t) =>
        logError("OpenLineage plugin failed to initialize; continuing in disabled mode", t)
        Collections.emptyMap[String, String]()
    }
  }

  /**
   * Factory for the event sink. Isolated so tests can subclass the plugin and
   * inject an in-memory sink without rebuilding the rest of `init`.
   *
   * Wiring policy:
   *   - `serviceUrl` unset → `EventSink.Noop`. Plugin is loaded but inert.
   *   - `serviceUrl` set   → `ConnectRpcEventSink` backed by a `LineageServiceClient`.
   */
  protected def buildSink(config: LineageConfig): EventSink = config.serviceUrl match {
    case None => EventSink.Noop
    case Some(url) =>
      val token = config.authToken.getOrElse("valid-token")
      val extraHeaders = Map("Authorization" -> s"Bearer $token")
      val transport = new ConnectRpcClient(
        baseUrl = url,
        okHttp = ConnectRpcClient.defaultOkHttp(),
        extraHeaders = extraHeaders
      )
      val svc       = new LineageServiceClient(transport)
      new ConnectRpcEventSink(
        client         = svc,
        queueCapacity  = config.queueSize,
        batchFlushMs   = config.batchFlushMs
      )
  }

  /**
   * Attach the QueryExecutionListener to the active SparkSession's listener
   * manager. If no active session exists yet (possible if the plugin is loaded
   * before the user constructs a SparkSession), we fall back to attaching via
   * a SparkListener that hooks on the first job start. For the common case
   * (SparkSession constructed before user code runs), the direct attach wins.
   */
  private def registerQueryListener(sc: SparkContext): Unit = {
    listener = new LineageQueryListener(
      config       = config,
      sparkConf    = sc.getConf,
      eventBuilder = new RunEventBuilder(),
      sink         = sink,
      metrics      = metricsAggregator
    )

    streamingListener = new LineageStreamingListener(
      config       = config,
      sparkConf    = sc.getConf,
      eventBuilder = new RunEventBuilder(),
      sink         = sink
    )

    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession) match {
      case Some(session) =>
        session.listenerManager.register(listener)
        // StreamingQueryListener attaches at the SparkSession level (via the
        // session's streams manager), not the plan listener manager.
        try session.streams.addListener(streamingListener)
        catch {
          case NonFatal(t) =>
            logWarning("OpenLineage StreamingQueryListener registration failed", t)
        }
        logInfo("OpenLineage listeners registered on active SparkSession")
      case None =>
        logInfo("No active SparkSession at plugin init; deferring listener registration")
        sc.addSparkListener(new DeferredListenerRegistrar(
          queryListener     = () => listener,
          streamingListener = () => streamingListener
        ))
    }
  }

  override def receive(message: Any): AnyRef = {
    message match {
      case m: ExecutorTaskMetrics =>
        try metricsAggregator.record(m)
        catch {
          case NonFatal(t) =>
            logWarning("OpenLineage failed to record ExecutorTaskMetrics", t)
        }
        null
      case other =>
        // Spark plugins sometimes see unexpected messages during driver
        // startup/shutdown races; we log at DEBUG to avoid polluting logs.
        logDebug(s"OpenLineage plugin received unexpected message type: ${Option(other).map(_.getClass.getName).orNull}")
        null
    }
  }

  override def shutdown(): Unit = {
    metricsAggregator.clear()
    try sink.close() catch { case NonFatal(t) => logWarning("OpenLineage sink close failed", t) }
    logInfo("OpenLineage driver plugin shutting down")
  }
}

/**
 * SparkListener that attaches our QueryExecutionListener to the SparkSession as
 * soon as one exists. This covers the narrow window where the plugin's `init`
 * runs before user code creates a SparkSession — which can happen under
 * `--conf spark.plugins=...` on `spark-submit`.
 */
private final class DeferredListenerRegistrar(
    queryListener: () => LineageQueryListener,
    streamingListener: () => LineageStreamingListener
) extends org.apache.spark.scheduler.SparkListener
    with Logging {
  @volatile private var attached = false

  override def onJobStart(ev: org.apache.spark.scheduler.SparkListenerJobStart): Unit = {
    if (attached) return
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach { session =>
      try {
        session.listenerManager.register(queryListener())
        try session.streams.addListener(streamingListener())
        catch {
          case NonFatal(t) =>
            logWarning("OpenLineage deferred StreamingQueryListener registration failed", t)
        }
        attached = true
        logInfo("OpenLineage listeners registered on SparkSession (deferred)")
      } catch {
        case NonFatal(t) =>
          logWarning("OpenLineage deferred listener registration failed; will retry on next job", t)
      }
    }
  }
}
