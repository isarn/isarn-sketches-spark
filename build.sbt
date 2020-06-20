// xsbt clean unidoc previewSite
// xsbt clean unidoc ghpagesPushSite
// xsbt +publish
// https://oss.sonatype.org
// make sure sparkVersion is set as you want prior to +publish

import scala.sys.process._

name := "isarn-sketches-spark"

organization := "org.isarnproject"

val packageVersion = "0.4.0"

val sparkVersion = "3.0.0"

val sparkSuffix = s"""sp${sparkVersion.split('.').take(2).mkString(".")}"""

version := s"${packageVersion}-${sparkSuffix}"

scalaVersion := "2.12.11"

crossScalaVersions := Seq("2.12.11") // scala 2.12.11 when spark supports it

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

libraryDependencies ++= Seq(
  "org.isarnproject" %% "isarn-sketches" % "0.1.2",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.isarnproject" %% "isarn-scalatest" % "0.0.3" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.apache.commons" % "commons-math3" % "3.6.1" % Test)

initialCommands in console := """
  |import org.apache.spark.SparkConf
  |import org.apache.spark.SparkContext
  |import org.apache.spark.sql.SparkSession
  |import org.apache.spark.SparkContext._
  |import org.apache.spark.rdd.RDD
  |import org.apache.spark.ml.linalg.Vectors
  |import org.isarnproject.sketches.TDigest
  |import org.isarnproject.sketches.udaf._
  |import org.apache.spark.isarnproject.sketches.udt._
  |val initialConf = new SparkConf().setAppName("repl").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "16mb")
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
  (baseDirectory.value / "python" / "isarnproject" / "pipelines" / "fi.py") -> "isarnproject/pipelines/fi.py",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "__init__.py") -> "isarnproject/sketches/__init__.py",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "udaf" / "__init__.py") -> "isarnproject/sketches/udaf/__init__.py",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "udaf" / "tdigest.py") -> "isarnproject/sketches/udaf/tdigest.py",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "udt" / "__init__.py") -> "isarnproject/sketches/udt/__init__.py",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "udt" / "tdigest.py") -> "isarnproject/sketches/udt/tdigest.py"
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
