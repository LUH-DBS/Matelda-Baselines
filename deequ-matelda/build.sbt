ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

libraryDependencies += "com.amazon.deequ" % "deequ" % "2.0.7-spark-3.5"

lazy val root = (project in file("."))
  .settings(
    name := "deequ-matelda"
  )
