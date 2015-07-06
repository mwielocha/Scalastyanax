import sbt._
import sbt.Keys._
import scala.Some
import scala.Some

object ScalastyanaxBuild extends Build {

  object V {
    val astyanax = "1.56.+"
  }

  val appName         = "Scalastyanax"
  val appVersion      = "3.0.0-SNAPSHOT"

  val appDependencies =  Seq(
    "com.netflix.astyanax" % "astyanax-core" % V.astyanax /*exclude("org.slf4j", "slf4j-log4j12")*/ % "provided",
    "com.netflix.astyanax" % "astyanax-thrift" % V.astyanax % "provided" exclude("javax.servlet", "servlet-api"),
    "com.netflix.astyanax" % "astyanax-entity-mapper" % V.astyanax % "provided" /*exclude("org.slf4j", "slf4j-log4j12")*/,
    "com.google.code.findbugs" % "jsr305" % "1.3.+",
    /*"org.cassandraunit" % "cassandra-unit" % "1.1.1.2" % "test"
      exclude("org.apache.cassandra", "cassandra-all") exclude("org.hectorclient", "hector-core"),*/
    "org.specs2" %% "specs2" % "2.1.1" % "test",
    "org.hectorclient" % "hector-core" % "1.1-4" % "test"
    /*"org.scala-lang" % "scala-reflect" % "2.10.3"*/
  )

  val buildSettings = Defaults.defaultSettings ++ Seq(
    version := appVersion,
    organization := "scalastyanax",
    exportJars := true,
    scalaVersion := "2.10.4"
  )

  val main = Project(id = appName, base = file("."),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= appDependencies
    )
  )
}
