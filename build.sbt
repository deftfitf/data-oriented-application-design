import Dependencies._

ThisBuild / scalaVersion     := "2.13.1"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val commonSettings = Seq(
  libraryDependencies += scalaTest % Test
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "data-oriented-application-design"
  ).aggregate(simpleKvs)

lazy val simpleKvs = (project in file("simple-kvs"))
  .settings(
    commonSettings,
    name := "simple-kvs"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
