name := "isarn-sketches-spark"

organization := "org.isarnproject"

bintrayOrganization := Some("isarn")

version := "0.1.0.py1"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

def commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.isarnproject" %% "isarn-sketches" % "0.1.0",
    "org.apache.spark" %% "spark-core" % "2.1.0" % Provided,
    "org.apache.spark" %% "spark-sql" % "2.1.0" % Provided,
    "org.apache.spark" %% "spark-mllib" % "2.1.0" % Provided,
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

lazy val compilePython = taskKey[Unit]("Compile python files")

compilePython := {
  val s: TaskStreams = streams.value
  s.log.info("compiling python...")
  val stat = (Seq("python2", "-m compileall python/") !)
  if (stat == 0) {
    s.log.info("python compile succeeded")
  } else {
    throw new IllegalStateException("python compile failed")
  }
}

(run in Compile) <<= (run in Compile).dependsOn(compilePython)

mappings in (Compile, packageBin) += {
  (baseDirectory.value / "python" / "isarnproject" / "sketches" / "udaf" / "tdigest.py") -> "isarnproject/sketches/udaf/tdigest.py"
}

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
