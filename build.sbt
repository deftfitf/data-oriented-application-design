import Dependencies._

lazy val commonSettings = Seq(
  scalaVersion     := "2.13.1",
  version          := "0.1.0-SNAPSHOT",
  organization     := "com.example",
  organizationName := "example",
  scalacOptions in Compile ++= Seq(
    "-feature",
    "-deprecation",
    "-unchecked",
    "-encoding",
    "UTF-8",
    "-Xfatal-warnings",
    "-Xlint",
    "-language:existentials",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-language:higherKinds",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused",
    "-Ywarn-value-discard",
  ),
  scalacOptions in console -= "-Ywarn-unused-import",
  scalacOptions in Test ++= Seq("-Yrangepos"),
  libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor-typed" % "2.6.0",
    "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime,
    "org.specs2" %% "specs2-core" % "4.8.0" % Test,
    "org.specs2" %% "specs2-mock" % "4.8.0" % Test,
    scalaTest % Test
  ),
  scalafmtOnCompile in ThisBuild := true,
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "data-oriented-application-design"
  ).aggregate(simpleKvs, simpleLSMKvs)

lazy val simpleKvs = (project in file("simple-kvs"))
  .settings(
    commonSettings,
    name := "simple-kvs"
  )

lazy val simpleLSMKvs = (project in file("simple-lsm-kvs"))
  .settings(
    commonSettings,
    name := "simple-lsm-kvs"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
