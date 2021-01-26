// sbt clean unidoc previewSite
// sbt clean unidoc ghpagesPushSite
// sbt +publish
// https://oss.sonatype.org
// make sure sparkVersion is set as you want prior to +publish
// when doing localPublish, also do:
// rm -rf /home/eje/.ivy2/local/org.isarnproject /home/eje/.ivy2/cache/org.isarnproject

import scala.sys.process._

name := "isarn-sketches-spark"

organization := "org.isarnproject"

val packageVersion = "0.5.1"

val sparkVersion = "3.0.1"

val sparkSuffix = s"""sp${sparkVersion.split('.').take(2).mkString(".")}"""

version := s"${packageVersion}-${sparkSuffix}"

scalaVersion := "2.12.11"

crossScalaVersions := Seq("2.12.11")

pomIncludeRepository := { _ => false }

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

homepage := Some(url("https://github.com/isarn/isarn-sketches-spark"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/isarn/isarn-sketches-spark"),
    "scm:git@github.com:isarn/isarn-sketches-spark.git"
  )
)

developers := List(
  Developer(
    id    = "erikerlandson",
    name  = "Erik Erlandson",
    email = "eje@redhat.com",
    url   = url("https://erikerlandson.github.io/")
  )
)

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.isarnproject" % "isarn-sketches-java" % "0.3.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "com.lihaoyi" %% "utest" % "0.7.4" % Test)

// tell sbt about utest
testFrameworks += new TestFramework("utest.runner.Framework")

// default is to run tests in parallel, asynchronously, but
// that breaks both spark-cluster setup and teardown, and also breaks
// repeatability of the random data generation
parallelExecution in Test := false

initialCommands in console := """
  |import org.apache.spark.SparkConf
  |import org.apache.spark.SparkContext
  |import org.apache.spark.sql.SparkSession
  |import org.apache.spark.SparkContext._
  |import org.apache.spark.rdd.RDD
  |import org.apache.spark.ml.linalg.Vectors
  |import org.apache.spark.sql.functions._
  |import org.isarnproject.sketches.java.TDigest
  |import org.isarnproject.sketches.spark._
  |val initialConf = new SparkConf().setAppName("repl")
  |val spark = SparkSession.builder.config(initialConf).master("local[2]").getOrCreate()
  |import spark._, spark.implicits._
  |val sc = spark.sparkContext
  |import org.apache.log4j.{Logger, ConsoleAppender, Level}
  |Logger.getRootLogger().getAppender("console").asInstanceOf[ConsoleAppender].setThreshold(Level.WARN)
""".stripMargin

cleanupCommands in console := "spark.stop"

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

mappings in (Compile, packageBin) ++= Seq(
  (baseDirectory.value / "python" / "isarnproject" / "__init__.py") -> "isarnproject/__init__.py",
  (baseDirectory.value / "python" / "isarnproject" / "pipelines" / "__init__.py") -> "isarnproject/pipelines/__init__.py",
  (baseDirectory.value / "python" / "isarnproject" / "pipelines" / "spark" /  "__init__.py") -> "isarnproject/pipelines/spark/__init__.py",
  (baseDirectory.value / "python" / "isarnproject" / "pipelines" / "spark" / "fi.py") -> "isarnproject/pipelines/spark/fi.py",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "__init__.py") -> "isarnproject/sketches/__init__.py",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "spark" / "__init__.py") -> "isarnproject/sketches/spark/__init__.py",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "spark" / "tdigest.py") -> "isarnproject/sketches/spark/tdigest.py",
)

test in assembly := {}

assemblyShadeRules in assembly := Seq(
  ShadeRule.zap("scala.**").inAll,
  ShadeRule.zap("org.slf4j.**").inAll
)

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/root-doc.txt")

enablePlugins(ScalaUnidocPlugin, GhpagesPlugin)

siteSubdirName in ScalaUnidoc := "latest/api"

addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc)

git.remoteRepo := "git@github.com:isarn/isarn-sketches-spark.git"
