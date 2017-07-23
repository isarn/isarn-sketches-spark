name := "isarn-sketches-spark"

organization := "org.isarnproject"

bintrayOrganization := Some("isarn")

val packageVersion = "0.1.0"

val sparkVersion = "2.1.0"

val pythonVersion = "2.7"

val sparkSuffix = s"""sp${sparkVersion.split('.').take(2).mkString("_")}"""

val pythonSuffix = s"""py${pythonVersion.split('.').take(2).mkString("_")}"""

val pythonCMD = s"""python${pythonVersion.split('.').head}"""

version := s"${packageVersion}.${sparkSuffix}_${pythonSuffix}"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

def commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.isarnproject" %% "isarn-sketches" % "0.1.0",
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
    "org.isarnproject" %% "isarn-scalatest" % "0.0.1" % Test,
    "org.scalatest" %% "scalatest" % "2.2.4" % Test,
    "org.apache.commons" % "commons-math3" % "3.6.1" % Test),
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
    """.stripMargin,
    cleanupCommands in console := "spark.stop"
)

seq(commonSettings:_*)

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

compilePython <<= compilePython.dependsOn(deletePYC)

(packageBin in Compile) <<= (packageBin in Compile).dependsOn(compilePython)

mappings in (Compile, packageBin) ++= Seq(
  (baseDirectory.value / "python" / "isarnproject" / "__init__.pyc") -> "isarnproject/__init__.pyc",
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

site.settings

site.includeScaladoc()

// Re-enable if/when we want to support gh-pages w/ jekyll
// site.jekyllSupport()

ghpages.settings

git.remoteRepo := "git@github.com:isarn/isarn-sketches-spark.git"
