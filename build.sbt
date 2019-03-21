// xsbt clean unidoc previewSite
// xsbt clean unidoc ghpagesPushSite
// xsbt +publish
// make sure sparkVersion and pythonVersion are set as you want them prior to +publish

import scala.sys.process._

name := "isarn-sketches-spark"

organization := "org.isarnproject"

val packageVersion = "0.4.0-SNAPSHOT"

val sparkVersion = "2.4.0"

val pythonVersion = "2.7"

val sparkSuffix = s"""sp${sparkVersion.split('.').take(2).mkString(".")}"""

val pythonSuffix = s"""py${pythonVersion.split('.').take(2).mkString(".")}"""

val pythonCMD = s"""python${pythonVersion.split('.').head}"""

version := s"${packageVersion}-${sparkSuffix}-${pythonSuffix}"

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.11.12") // scala 2.12 when spark supports it

pomIncludeRepository := { _ => false }

//isSnapshot := true

//publishConfiguration := publishConfiguration.value.withOverwrite(true)

//publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

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
  "org.isarnproject" % "isarn-sketches-java" % "0.2.2-LOCAL",
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
  |import org.isarnproject.sketches.java.TDigest
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

lazy val deletePYC = taskKey[Unit]("Delete .pyc files")

deletePYC := {
  val s: TaskStreams = streams.value
  s.log.info("delete .pyc files...")
  val cmd = "bash" :: "-c" :: "rm -f $(find python -name *.pyc)" :: Nil
  val stat = (cmd !)
  if (stat == 0) {
    s.log.info("delete .pyc succeeded")
  } else {
    throw new IllegalStateException("delete .pyc failed")
  }
}

lazy val compilePython = taskKey[Unit]("Compile python files")

compilePython := {
  val s: TaskStreams = streams.value
  s.log.info("compiling python...")
  val stat = (Seq(pythonCMD, "-m", "compileall", "python/") !)
  if (stat == 0) {
    s.log.info("python compile succeeded")
  } else {
    throw new IllegalStateException("python compile failed")
  }
}

compilePython := (compilePython.dependsOn(deletePYC)).value

(packageBin in Compile) := ((packageBin in Compile).dependsOn(compilePython)).value

mappings in (Compile, packageBin) ++= Seq(
  (baseDirectory.value / "python" / "isarnproject" / "__init__.pyc") -> "isarnproject/__init__.pyc",
  (baseDirectory.value / "python" / "isarnproject" / "pipelines" / "__init__.pyc") -> "isarnproject/pipelines/__init__.pyc",
  (baseDirectory.value / "python" / "isarnproject" / "pipelines" / "fi.pyc") -> "isarnproject/pipelines/fi.pyc",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "__init__.pyc") -> "isarnproject/sketches/__init__.pyc",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "udaf" / "__init__.pyc") -> "isarnproject/sketches/udaf/__init__.pyc",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "udaf" / "tdigest.pyc") -> "isarnproject/sketches/udaf/tdigest.pyc",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "udt" / "__init__.pyc") -> "isarnproject/sketches/udt/__init__.pyc",
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "udt" / "tdigest.pyc") -> "isarnproject/sketches/udt/tdigest.pyc"
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
