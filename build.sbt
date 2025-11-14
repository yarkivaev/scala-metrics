ThisBuild / version           := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion      := "3.3.1"
ThisBuild / organization      := "io.github.yarkivaev"
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// Publishing settings for Maven Central
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / licenses      := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))
ThisBuild / homepage      := Some(url("https://github.com/yarkivaev/metrics-dsl"))
ThisBuild / scmInfo       := Some(
  ScmInfo(
    url("https://github.com/yarkivaev/metrics-dsl"),
    "scm:git@github.com:yarkivaev/metrics-dsl.git",
  ),
)
ThisBuild / developers    := List(
  Developer(
    id = "yarkivaev",
    name = "Yaroslav Kivaev",
    email = "", // Add email if you want
    url = url("https://github.com/yarkivaev"),
  ),
)

// Common settings
lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xfatal-warnings",
  ),
  // Wartremover settings - lenient for DSL library with intentional runtime type safety
  Compile / compile / wartremoverErrors ++= Seq(
    Wart.EitherProjectionPartial,
    Wart.OptionPartial,
    Wart.TripleQuestionMark,
  ),
  Compile / compile / wartremoverWarnings ++= Seq(
    Wart.ArrayEquals,
    Wart.Enumeration,
    Wart.ImplicitConversion,
    Wart.JavaConversions,
  ),
)

// metrics-dsl-core: Pure DSL with minimal dependencies
// Contains: core domain model, DSL syntax, algebra traits
lazy val core = (project in file("core"))
  .settings(
    name := "metrics-dsl-core",
    commonSettings,
    libraryDependencies ++= Seq(
      // fs2 for streaming
      "co.fs2" %% "fs2-core" % "3.9.3",

      // cats-effect for effect types
      "org.typelevel" %% "cats-effect" % "3.5.2",

      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
    ),
  )

// metrics-dsl-runtime: Implementations and data sources
// Contains: interpreters, data sources (CSV, in-memory, etc.)
lazy val runtime = (project in file("runtime"))
  .dependsOn(core)
  .settings(
    name := "metrics-dsl-runtime",
    commonSettings,
    libraryDependencies ++= Seq(
      // CSV/Excel handling
      "com.github.tototoshi" %% "scala-csv" % "1.3.10",
      "org.apache.poi"        % "poi"       % "5.2.5",
      "org.apache.poi"        % "poi-ooxml" % "5.2.5",

      // fs2 for streaming (inherited from core, but explicit for clarity)
      "co.fs2"        %% "fs2-core"    % "3.9.3",
      "org.typelevel" %% "cats-effect" % "3.5.2",

      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
    ),
  )

// metrics-dsl-playground: Interactive REPL and scripting environment
// Contains: Convenience APIs, helpers, presets, example scripts
// For experimentation and testing - not for production use
lazy val playground = (project in file("playground"))
  .dependsOn(core, runtime)
  .settings(
    name                                := "metrics-dsl-playground",
    commonSettings,
    libraryDependencies ++= Seq(
      // PPrint for nice output formatting
      "com.lihaoyi" %% "pprint" % "0.8.1",

      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
    ),
    // Auto-import playground utilities in console
    Compile / console / initialCommands := """
      |import playground._
      |import playground.Implicits._
      |import metrics.dsl.FormulaDSL.{given, *}
      |import cats.effect.IO
      |import cats.effect.unsafe.implicits.global
      |println("=" * 60)
      |println("METRICS DSL PLAYGROUND - Ready!")
      |println("=" * 60)
      |println("")
      |println("Quick test:")
      |println("  val ds = data.inMemory[IO](\"a\" -> 10.0, \"b\" -> 20.0)")
      |println("  val f = formula.query[IO](\"a\") + formula.query[IO](\"b\")")
      |println("  eval(f, ds)")
      |println("")
      |println("=" * 60)
    """.stripMargin,
  )

// Root project aggregates all subprojects
lazy val root = (project in file("."))
  .aggregate(core, runtime, playground)
  .settings(
    name           := "metrics-dsl",
    publish / skip := true,// Don't publish the root aggregate
  )
