import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / organization := "com.openlakehouse"
ThisBuild / version      := "0.1.0-SNAPSHOT"

// Spark 4.x is Scala 2.13 only — no cross-build.
ThisBuild / scalaVersion := "2.13.14"

// Spark 4 requires JDK 17 minimum.
ThisBuild / javacOptions  ++= Seq("-source", "17", "-target", "17")
ThisBuild / scalacOptions ++= Seq(
  "-release", "17",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint:-unused,_",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen"
)

// Versions are pinned here so the whole plugin recompiles cleanly against one
// Spark 4.1.x patch level. Bump deliberately and re-run the API audit
// (see .cursor/plans/spark_openlineage_plugin_*.plan.md — todo `spark4-api-audit`).
val sparkVersion          = "4.1.1"
val deltaVersion          = "4.0.0"     // delta-spark built for Spark 4.x (Scala 2.13)
val connectKotlinVersion  = "0.7.2"     // connect-kotlin-okhttp (Java-callable from Scala)
// Generated Java proto classes from buf.build/protocolbuffers/java use the
// protobuf-java 4.x "GeneratedFile" runtime API (RuntimeVersion validation).
// Must stay aligned with the --protobuf_java_version the buf remote plugin targets.
val protobufVersion       = "4.34.1"
// protovalidate-java provides build.buf.validate.* classes referenced by our
// generated Lineage.java (the proto has buf.validate.field annotations).
val protovalidateVersion  = "0.7.0"
// OkHttp is declared explicitly (not just pulled transitively via connect-kotlin)
// because our `ConnectRpcClient` drives the Connect unary protocol over raw
// OkHttp — talking to the connect-kotlin ProtocolClient from Scala is awkward
// due to Kotlin `suspend` functions, so we bypass it and use HTTP directly.
val okhttpVersion         = "4.12.0"
val scalatestVersion      = "3.2.19"

lazy val root = (project in file("."))
  .settings(
    name := "spark-openlineage-plugin",

    // Generated Java sources from `buf generate` land here; committed so
    // CI runners without `buf` can build.
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "main" / "generated",

    libraryDependencies ++= Seq(
      "org.apache.spark"    %% "spark-core"      % sparkVersion % Provided,
      "org.apache.spark"    %% "spark-sql"       % sparkVersion % Provided,
      "org.apache.spark"    %% "spark-streaming" % sparkVersion % Provided,
      // Only needed if the plugin is loaded inside a spark-connect-server JVM;
      // the server distribution already carries it so keep it provided.
      "org.apache.spark"    %% "spark-connect"   % sparkVersion % Provided,

      "com.google.protobuf"  % "protobuf-java"    % protobufVersion,
      // Provides build.buf.validate.* referenced by the generated Lineage.java
      // (our .proto uses buf.validate.field annotations).
      "build.buf"            % "protovalidate"    % protovalidateVersion,
      // ConnectRPC is currently only shipped as a Kotlin library; it's Java-callable
      // from Scala. Pulls kotlin-stdlib + okhttp transitively (both shaded below).
      // We keep it on the classpath because the generated Lineage.java references
      // com.connectrpc.Code for error codes, but we do NOT call ProtocolClient —
      // see ConnectRpcClient.scala for the rationale.
      "com.connectrpc"       % "connect-kotlin"         % connectKotlinVersion,
      "com.connectrpc"       % "connect-kotlin-okhttp"  % connectKotlinVersion,
      // Our Connect transport. Listed explicitly so dep resolution doesn't
      // accidentally downgrade us when connect-kotlin bumps its OkHttp floor.
      "com.squareup.okhttp3" % "okhttp"                 % okhttpVersion,

      "org.scalatest"        %% "scalatest"            % scalatestVersion % Test,
      "org.apache.spark"     %% "spark-core"           % sparkVersion     % Test classifier "tests",
      "org.apache.spark"     %% "spark-sql"            % sparkVersion     % Test classifier "tests",
      "io.delta"             %% "delta-spark"          % deltaVersion     % Test,
      // MockWebServer gives us a real local HTTP server in tests so we exercise
      // the full Connect wire format (headers + proto body) without mocking OkHttp.
      "com.squareup.okhttp3" % "mockwebserver"         % okhttpVersion    % Test
    ),

    // Shading is non-optional. Spark 4 still bundles its own protobuf-java, and
    // user jobs pull conflicting versions via Hadoop/Hive/Kafka.
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "com.openlakehouse.shaded.protobuf.@1").inAll,
      ShadeRule.rename("com.connectrpc.**"      -> "com.openlakehouse.shaded.connectrpc.@1").inAll,
      ShadeRule.rename("okhttp3.**"             -> "com.openlakehouse.shaded.okhttp3.@1").inAll,
      ShadeRule.rename("okio.**"                -> "com.openlakehouse.shaded.okio.@1").inAll,
      // Kotlin stdlib is pulled in transitively by connect-kotlin. Leaving it
      // unshaded would collide with user jobs that embed their own Kotlin.
      ShadeRule.rename("kotlin.**"              -> "com.openlakehouse.shaded.kotlin.@1").inAll,
      ShadeRule.rename("kotlinx.**"             -> "com.openlakehouse.shaded.kotlinx.@1").inAll
    ),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.toLowerCase.endsWith(".sf"))   => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.toLowerCase.endsWith(".dsa"))  => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.toLowerCase.endsWith(".rsa"))  => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.lastOption.contains("services")                     => MergeStrategy.concat
      case "module-info.class"                                                                     => MergeStrategy.discard
      case PathList("META-INF", xs @ _*)                                                           => MergeStrategy.first
      case _                                                                                       => MergeStrategy.first
    },

    // Spark classes leak into every UDF closure; forking gives us clean test classpaths
    // and lets Spark 4 negotiate its own JVM module-access flags.
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      // Spark 4 + JDK 17 still needs the classic set of --add-opens flags.
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),
    Test / parallelExecution := false
  )
