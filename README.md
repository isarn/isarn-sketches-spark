# isarn-sketches-spark
Routines and data structures for using isarn-sketches idiomatically in Apache Spark

## API documentation
https://isarn.github.io/isarn-sketches-spark/latest/api/#org.isarnproject.sketches.udaf.package

## How to use in your project

``` scala
// Note that the version of spark and python is part of the release name.
// This example is for spark 2.2 and python 2.7:
libraryDependencies += "org.isarnproject" %% "isarn-sketches-spark" % "0.3.1-sp2.2-py2.7"
```

** Currently supported: python 2.7, 3.6  X  spark 2.2, 2.3  X  scala 2.11 **

If you are interested in a python/spark/scala build that is not listed above, please contact me and/or file an issue!

This package builds against some `% Provided` Apache Spark dependencies:
```scala
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
```

## How to use from the Spark CLI
Several Spark CLI tools accept the `--packages` argument, as with this `spark-shell` example:
```bash
$ spark-shell --packages "org.isarnproject:isarn-sketches-spark_2.11:0.3.1-sp2.3-py3.6"
```
Note that you need to explicitly include the scala version as part of the package name

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

### Sketch a numeric column (python)
```python
>>> from isarnproject.sketches.udaf.tdigest import *
>>> from random import gauss
>>> from pyspark.sql.types import *
>>> data = sc.parallelize([[gauss(0,1)] for x in xrange(1000)]).toDF(StructType([StructField("x", DoubleType())]))
>>> agg = data.agg(tdigestDoubleUDAF("x"))
>>> td = agg.first()[0]
>>> td.cdfInverse(0.5)
0.046805581998797419
>>> 
```

### Sketch a numeric array column (python)
```python
>>> from isarnproject.sketches.udaf.tdigest import *
>>> from random import gauss
>>> from pyspark.sql.types import *
>>> data = sc.parallelize([[[gauss(0,1),gauss(0,1),gauss(0,1)]] for x in xrange(1000)]).toDF(StructType([StructField("x", ArrayType(DoubleType()))]))
>>> agg = data.agg(tdigestDoubleArrayUDAF("x"))
>>> tds = agg.first()[0]
>>> [t.cdfInverse(0.5) for t in td] 
[0.046116924117141189, -0.011071666930287466, -0.019006033872431105]
>>> 
```

### Sketch a column of ML Vectors (python)
```python
>>> from isarnproject.sketches.udaf.tdigest import *
>>> from random import gauss
>>> from pyspark.ml.linalg import VectorUDT, Vectors
>>> from pyspark.sql.types import *
>>> data = sc.parallelize([[Vectors.dense([gauss(0,1),gauss(0,1),gauss(0,1)])] for x in xrange(1000)]).toDF(StructType([StructField("x", VectorUDT())]))
>>> agg = data.agg(tdigestMLVecUDAF("x"))
>>> tds = agg.first()[0]
>>> [t.cdfInverse(0.5) for t in tds]
[0.02859498787770634, -0.0027338622700039117, 0.041590980872883487]
>>> 
```

### Sketch a column of MLLib Vectors (python)
```python
>>> from isarnproject.sketches.udaf.tdigest import *
>>> from random import gauss
>>> from pyspark.mllib.linalg import VectorUDT, Vectors
>>> from pyspark.sql.types import *
>>> data = sc.parallelize([[Vectors.dense([gauss(0,1),gauss(0,1),gauss(0,1)])] for x in xrange(1000)]).toDF(StructType([StructField("x", VectorUDT())]))
>>> agg = data.agg(tdigestMLLibVecUDAF("x"))
>>> tds = agg.first()[0]
>>> [t.cdfInverse(0.5) for t in tds]
[0.02859498787770634, -0.0027338622700039117, 0.041590980872883487]
>>>
```

### Reduce a column (or grouping) of T-Digests (python)
```python
>>> from isarnproject.sketches.udaf.tdigest import *
>>> from random import gauss
>>> from pyspark.sql.types import *
>>> x = sc.parallelize([[gauss(0,1)] for x in xrange(1000)]).toDF(StructType([StructField("x", DoubleType())]))
>>> g = sc.parallelize([[1+x] for x in xrange(5)]).toDF(StructType([StructField("g", IntegerType())]))
>>> data = g.crossJoin(x)
>>> tds = data.groupBy("g").agg(tdigestDoubleUDAF("x").alias("tdigests"))
>>> tds.show()
+---+--------------------+                                                      
|  g|            tdigests|
+---+--------------------+
|  1|TDigestSQL(TDiges...|
|  3|TDigestSQL(TDiges...|
|  5|TDigestSQL(TDiges...|
|  4|TDigestSQL(TDiges...|
|  2|TDigestSQL(TDiges...|
+---+--------------------+

>>> td = tds.agg(tdigestReduceUDAF("tdigests").alias("tdigest"))
>>> td.show()
+--------------------+                                                          
|             tdigest|
+--------------------+
|TDigestSQL(TDiges...|
+--------------------+

>>> 
```

### Reduce a column (or grouping) of T-Digest Arrays (python)
```python
>>> from isarnproject.sketches.udaf.tdigest import *
>>> from random import gauss
>>> from pyspark.ml.linalg import VectorUDT, Vectors
>>> from pyspark.sql.types import *
>>> x = sc.parallelize([[Vectors.dense([gauss(0,1),gauss(0,1),gauss(0,1)])] for x in xrange(1000)]).toDF(StructType([StructField("x", VectorUDT())]))
>>> g = sc.parallelize([[1+x] for x in xrange(5)]).toDF(StructType([StructField("g", IntegerType())]))
>>> data = g.crossJoin(x)
>>> tds = data.groupBy("g").agg(tdigestMLVecUDAF("x").alias("tdigests"))
>>> tds.show()
+---+--------------------+                                                      
|  g|            tdigests|
+---+--------------------+
|  1|TDigestArraySQL([...|
|  3|TDigestArraySQL([...|
|  5|TDigestArraySQL([...|
|  4|TDigestArraySQL([...|
|  2|TDigestArraySQL([...|
+---+--------------------+

>>> td = tds.agg(tdigestArrayReduceUDAF("tdigests").alias("tdigest"))
>>> td.show()
+--------------------+                                                          
|             tdigest|
+--------------------+
|TDigestArraySQL([...|
+--------------------+
```

### Compute feature importance with respect to a predictive model
```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)

import org.apache.spark.ml.regression.LinearRegression

val training = spark.read.format("libsvm")
  .load("data/mllib/sample_linear_regression_data.txt")

val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

val lrModel = lr.fit(training)

import org.isarnproject.pipelines.{TDigestFI,TDigestFIModel}

val fi = new TDigestFI().setDelta(0.3).setMaxDiscrete(10)

val fiMod = fi.fit(training)
  .setTargetModel(lrModel)
  .setDeviationMeasure("rms-dev")
  .setFeatureNames(Array.tabulate(10){j=>s"x$j"})

val imp = fiMod.transform(training)

// Exiting paste mode, now interpreting.

import org.apache.spark.ml.regression.LinearRegression
training: org.apache.spark.sql.DataFrame = [label: double, features: vector]
lr: org.apache.spark.ml.regression.LinearRegression = linReg_ad8ebef9cfe8
lrModel: org.apache.spark.ml.regression.LinearRegressionModel = linReg_ad8ebef9cfe8
import org.isarnproject.pipelines.{TDigestFI, TDigestFIModel}
fi: org.isarnproject.pipelines.TDigestFI = TDigestFI_67b1cff93349
fiMod: org.isarnproject.pipelines.TDigestFIModel = TDigestFI_67b1cff93349
imp: org.apache.spark.sql.DataFrame = [name: string, importance: double]

scala> imp.show
+----+-------------------+
|name|         importance|
+----+-------------------+
|  x0|                0.0|
|  x1|0.27093413867331134|
|  x2|0.27512986364699304|
|  x3| 1.4284480425303374|
|  x4|0.04472982597939822|
|  x5| 0.5981079647203551|
|  x6|                0.0|
|  x7|0.11970670592684969|
|  x8| 0.1668815037423663|
|  x9|0.17970574939101025|
+----+-------------------+
```

### Compute feature importance with respect to a predictive model (python)
```python
>>> from pyspark.ml.regression import LinearRegression
>>> training = spark.read.format("libsvm") \
...     .load("data/mllib/sample_linear_regression_data.txt")
>>> lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
>>> lrModel = lr.fit(training)
>>> from isarnproject.pipelines.fi import *
>>> fi = TDigestFI().setDelta(0.3).setMaxDiscrete(10)
>>> fiMod = fi.fit(training) \
...     .setTargetModel(lrModel) \
...     .setDeviationMeasure("rms-dev") \
...     .setFeatureNames(["x%d" % (j) for j in xrange(10)])
>>> imp = fiMod.transform(training)
>>> imp.show()
+----+-------------------+
|name|         importance|
+----+-------------------+
|  x0|                0.0|
|  x1| 0.2513147892886899|
|  x2|0.28992477834838837|
|  x3| 1.4906022974248356|
|  x4|0.04197189119745892|
|  x5| 0.6213459845972947|
|  x6|                0.0|
|  x7|0.12463038543257152|
|  x8|0.17144699470039335|
|  x9|0.18428188512840307|
+----+-------------------+
```
