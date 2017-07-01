name := "isarn-sketches-spark"

organization := "org.isarnproject"

bintrayOrganization := Some("isarn")

version := "0.1.0.dev9"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

def commonSettings = Seq(
  resolvers += "Will's bintray" at "https://dl.bintray.com/willb/maven/",
  libraryDependencies ++= Seq(
    "org.isarnproject" %% "isarn-sketches" % "0.1.0",
    "com.redhat.et" %% "silex" % "0.1.2",
    "org.apache.spark" %% "spark-core" % "2.1.0" % Provided,
    "org.apache.spark" %% "spark-sql" % "2.1.0" % Provided,
    "org.apache.spark" %% "spark-mllib" % "2.1.0" % Provided,
    "org.isarnproject" %% "isarn-scalatest" % "0.0.1" % Test,
    "org.scalatest" %% "scalatest" % "2.2.4" % Test,
    "org.apache.commons" % "commons-math3" % "3.6.1" % Test),
    initialCommands in console := """
      |import org.apache.spark.SparkConf
      |import org.apache.spark.SparkContext
      |import org.apache.spark.SparkContext._
      |import org.apache.spark.rdd.RDD
      |import org.apache.spark.ml.linalg.Vectors
      |import org.isarnproject.sketches.udaf._
      |import org.apache.spark.isarnproject.sketches.udt._
      |val app = new com.redhat.et.silex.app.ConsoleApp()
      |val spark = app.spark
      |val sc = app.context
      |import spark._
      |import org.apache.log4j.{Logger, ConsoleAppender, Level}
      |Logger.getRootLogger().getAppender("console").asInstanceOf[ConsoleAppender].setThreshold(Level.WARN)
    """.stripMargin,
    cleanupCommands in console := "spark.stop"    
)

seq(commonSettings:_*)

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/root-doc.txt")

site.settings

site.includeScaladoc()

// Re-enable if/when we want to support gh-pages w/ jekyll
// site.jekyllSupport()

ghpages.settings

git.remoteRepo := "git@github.com:isarn/isarn-sketches-spark.git"
