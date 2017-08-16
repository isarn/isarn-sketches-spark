# isarn-sketches-spark
Routines and data structures for using isarn-sketches idiomatically in Apache Spark

## API documentation
https://isarn.github.io/isarn-sketches-spark/latest/api/#org.isarnproject.sketches.udaf.package

## How to use in your project

#### sbt
``` scala
resolvers += "isarn project" at "https://dl.bintray.com/isarn/maven/"

libraryDependencies += "org.isarnproject" %% "isarn-sketches-spark" % "0.1.0"
```

#### maven
``` xml
<dependency> 
  <groupId>org.isarnproject</groupId>
  <artifactId>isarn-sketches-spark_2.10</artifactId> 
  <version>0.1.0</version>
  <type>pom</type> 
</dependency>
```

## Examples

### Sketch a numeric column
```scala
scala> import org.isarnproject.sketches._, org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._
import org.isarnproject.sketches._
import org.isarnproject.sketches.udaf._
import org.apache.spark.isarnproject.sketches.udt._

scala> import scala.util.Random.nextGaussian
import scala.util.Random.nextGaussian

scala> val data = sc.parallelize(Vector.fill(1000){(nextGaussian, nextGaussian)}).toDF.as[(Double, Double)]
data: org.apache.spark.sql.Dataset[(Double, Double)] = [_1: double, _2: double]

scala> val udaf = tdigestUDAF[Double].delta(0.2).maxDiscrete(25)
udaf: org.isarnproject.sketches.udaf.TDigestUDAF[Double] = TDigestUDAF(0.2,25)

scala> val agg = data.agg(udaf($"_1"), udaf($"_2"))
agg: org.apache.spark.sql.DataFrame = [tdigestudaf(_1): tdigest, tdigestudaf(_2): tdigest]

scala> val (td1, td2) = (agg.first.getAs[TDigestSQL](0).tdigest, agg.first.getAs[TDigestSQL](1).tdigest)
td1: org.isarnproject.sketches.TDigest = TDigest(0.2,25,151,TDigestMap(-3.1241237514093707 -> (1.0, 1.0), ...

scala> td1.cdf(0)
res1: Double = 0.5159531867457404

scala> td2.cdf(0)
res2: Double = 0.504233763693618
```

### Sketch a numeric array column
```scala
scala> import org.isarnproject.sketches._, org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._
import org.isarnproject.sketches._
import org.isarnproject.sketches.udaf._
import org.apache.spark.isarnproject.sketches.udt._

scala> import scala.util.Random._
import scala.util.Random._

scala> val data = spark.createDataFrame(Vector.fill(1000){(nextInt(10), Vector.fill(5){nextGaussian})})
data: org.apache.spark.sql.DataFrame = [_1: int, _2: array<double>]

scala> val udaf1 = tdigestUDAF[Int].maxDiscrete(20)
udaf1: org.isarnproject.sketches.udaf.TDigestUDAF[Int] = TDigestUDAF(0.5,20)

scala> val udafA = tdigestArrayUDAF[Double]
udafA: org.isarnproject.sketches.udaf.TDigestArrayUDAF[Double] = TDigestArrayUDAF(0.5,0)

scala> val (first1, firstA) = (data.agg(udaf1($"_1")).first, data.agg(udafA($"_2")).first)
first1: org.apache.spark.sql.Row = [TDigestSQL(TDigest(0.5,20,19,TDigestMap(-9.0 -> (51.0, 51.0),...
firstA: org.apache.spark.sql.Row = [TDigestArraySQL([Lorg.isarnproject.sketches.TDigest;@782b0d37)]

scala> val sample1 = Vector.fill(10) { first1.getAs[TDigestSQL](0).tdigest.sample }
sample1: scala.collection.immutable.Vector[Double] = Vector(0.0, 7.0, 9.0, 6.0, 1.0, 3.0, 4.0, 0.0, 9.0, 0.0)

scala> val sampleA = firstA.getAs[TDigestArraySQL](0).tdigests.map(_.sample)
sampleA: Array[Double] = Array(0.5079398036724695, 0.7518583956493221, -0.054376728126603546, 0.7141623682043323, 0.4788564991204228)
```

### Sketch a column of ML Vector
```scala
scala> import org.isarnproject.sketches._, org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._
import org.isarnproject.sketches._
import org.isarnproject.sketches.udaf._
import org.apache.spark.isarnproject.sketches.udt._

scala> import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vectors

scala> import scala.util.Random._
import scala.util.Random._

scala> val data = spark.createDataFrame(Vector.fill(1000){(nextInt(10), Vectors.dense(nextGaussian,nextGaussian,nextGaussian))})
data: org.apache.spark.sql.DataFrame = [_1: int, _2: vector]

scala> val udafV = tdigestMLVecUDAF
udafV: org.isarnproject.sketches.udaf.TDigestMLVecUDAF = TDigestMLVecUDAF(0.5,0)

scala> val firstV = data.agg(udafV($"_2")).first
firstV: org.apache.spark.sql.Row = [TDigestArraySQL([Lorg.isarnproject.sketches.TDigest;@42b579cd)]

scala> val sampleV = firstV.getAs[TDigestArraySQL](0).tdigests.map(_.sample)
sampleV: Array[Double] = Array(1.815862652134914, 0.24668895676164276, 0.09236479932949887)

scala> val medianV = firstV.getAs[TDigestArraySQL](0).tdigests.map(_.cdfInverse(0.5))
medianV: Array[Double] = Array(-0.049806905959001196, -0.08528817932077674, -0.05291800642695017)
```

### Sketch a column of MLLib Vector
```scala
scala> import org.isarnproject.sketches._, org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._
import org.isarnproject.sketches._
import org.isarnproject.sketches.udaf._
import org.apache.spark.isarnproject.sketches.udt._

scala> import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors

scala> import scala.util.Random._
import scala.util.Random._

scala> val data = spark.createDataFrame(Vector.fill(1000){(nextInt(10), Vectors.dense(nextGaussian,nextGaussian,nextGaussian))})
data: org.apache.spark.sql.DataFrame = [_1: int, _2: vector]

scala> val udafV = tdigestMLLibVecUDAF
udafV: org.isarnproject.sketches.udaf.TDigestMLLibVecUDAF = TDigestMLLibVecUDAF(0.5,0)

scala> val firstV = data.agg(udafV($"_2")).first
firstV: org.apache.spark.sql.Row = [TDigestArraySQL([Lorg.isarnproject.sketches.TDigest;@6bffea90)]

scala> val sampleV = firstV.getAs[TDigestArraySQL](0).tdigests.map(_.sample)
sampleV: Array[Double] = Array(0.10298190759496548, -0.1968752746464183, -1.0139250851274562)

scala> val medianV = firstV.getAs[TDigestArraySQL](0).tdigests.map(_.cdfInverse(0.5))
medianV: Array[Double] = Array(0.025820266848484798, 0.01951778217339037, 0.09701138847692858)
```

### Reduce a column (or grouping) of T-Digests
```scala
scala> import org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._, org.isarnproject.sketches._, scala.util.Random._
import org.isarnproject.sketches.udaf._
import org.apache.spark.isarnproject.sketches.udt._
import org.isarnproject.sketches._
import scala.util.Random._

scala> val x = sc.parallelize(Vector.fill(1000) { nextGaussian }).toDF("x")
x: org.apache.spark.sql.DataFrame = [x: double]

scala> val g = sc.parallelize(Seq(1,2,3,4,5)).toDF("g")
g: org.apache.spark.sql.DataFrame = [g: int]

scala> val data = g.crossJoin(x)
data: org.apache.spark.sql.DataFrame = [g: int, x: double]

scala> val udaf = tdigestUDAF[Double]
udaf: org.isarnproject.sketches.udaf.TDigestUDAF[Double] = TDigestUDAF(0.5,0)

scala> val tds = data.groupBy("g").agg(udaf($"x").alias("tdigests"))
tds: org.apache.spark.sql.DataFrame = [g: int, tdigests: tdigest]

scala> tds.show()
+---+--------------------+
|  g|            tdigests|
+---+--------------------+
|  1|TDigestSQL(TDiges...|
|  3|TDigestSQL(TDiges...|
|  5|TDigestSQL(TDiges...|
|  4|TDigestSQL(TDiges...|
|  2|TDigestSQL(TDiges...|
+---+--------------------+

scala> val td = tds.agg(tdigestReduceUDAF($"tdigests").alias("tdigest"))
td: org.apache.spark.sql.DataFrame = [tdigest: tdigest]

scala> td.show()
+--------------------+
|             tdigest|
+--------------------+
|TDigestSQL(TDiges...|
+--------------------+
```

### Reduce a column (or grouping) of T-Digest Arrays
```scala
scala> import org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._, org.isarnproject.sketches._, scala.util.Random._
import org.isarnproject.sketches.udaf._
import org.apache.spark.isarnproject.sketches.udt._
import org.isarnproject.sketches._
import scala.util.Random._

scala> val x = sc.parallelize(Vector.fill(1000) { Vector.fill(3) { nextGaussian } }).toDF("x")
x: org.apache.spark.sql.DataFrame = [x: array<double>]

scala> val g = sc.parallelize(Seq(1,2,3,4,5)).toDF("g")
g: org.apache.spark.sql.DataFrame = [g: int]

scala> val data = g.crossJoin(x)
data: org.apache.spark.sql.DataFrame = [g: int, x: array<double>]

scala> val udaf = tdigestArrayUDAF[Double]
udaf: org.isarnproject.sketches.udaf.TDigestArrayUDAF[Double] = TDigestArrayUDAF(0.5,0)

scala> val tds = data.groupBy("g").agg(udaf($"x").alias("tdigests"))
tds: org.apache.spark.sql.DataFrame = [g: int, tdigests: tdigestarray]

scala> tds.show()
+---+--------------------+
|  g|            tdigests|
+---+--------------------+
|  1|TDigestArraySQL([...|
|  3|TDigestArraySQL([...|
|  5|TDigestArraySQL([...|
|  4|TDigestArraySQL([...|
|  2|TDigestArraySQL([...|
+---+--------------------+

scala> val td = tds.agg(tdigestArrayReduceUDAF($"tdigests"))
td: org.apache.spark.sql.DataFrame = [tdigestarrayreduceudaf(tdigests): tdigestarray]

scala> td.show()
+--------------------------------+
|tdigestarrayreduceudaf(tdigests)|
+--------------------------------+
|            TDigestArraySQL([...|
+--------------------------------+
```
