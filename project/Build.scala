import sbt._
import Keys._

object ApplicationBuild extends Build {

  object V {
    val astyanax = "1.56.42"
  }

  val appName         = "Scalastyanax"
  val appVersion      = "2.0.0-SNAPSHOT"

  val appDependencies =  Seq(
    "com.netflix.astyanax" % "astyanax-core" % V.astyanax /*exclude("org.slf4j", "slf4j-log4j12")*/,
    "com.netflix.astyanax" % "astyanax-thrift" % V.astyanax exclude("javax.servlet", "servlet-api"),
    "com.netflix.astyanax" % "astyanax-entity-mapper" % V.astyanax /*exclude("org.slf4j", "slf4j-log4j12")*/,
    /*"org.cassandraunit" % "cassandra-unit" % "1.1.1.2" % "test"
      exclude("org.apache.cassandra", "cassandra-all") exclude("org.hectorclient", "hector-core"),*/
    "org.specs2" %% "specs2" % "2.1.1" % "test"
  )

  val buildSettings = Defaults.defaultSettings ++ Seq(
    version := appVersion,
    exportJars := true,
    scalaVersion        := "2.10.2"
  )

  val main = Project(id = appName, base = file("."),
    settings = buildSettings ++ Seq(libraryDependencies ++= appDependencies)
  )
}
