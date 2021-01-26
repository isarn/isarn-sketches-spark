# isarn-sketches-spark
Routines and data structures for using isarn-sketches idiomatically in Apache Spark

## API documentation
https://isarn.github.io/isarn-sketches-spark/latest/api/#org.isarnproject.sketches.spark

## How to use in your project

``` scala
// Note that the version of spark is part of the release name.
// This example is for spark 3.0:
libraryDependencies += "org.isarnproject" %% "isarn-sketches-spark" % "0.5.1-sp3.0"

// This package builds against some `% Provided` Apache Spark dependencies:
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
```

Python code for supporting `pyspark` is also packaged with all of the artifacts above.
Spark will automatically extract and compile Python components for use with PySpark,
as illustrated in pyspark examples below.
Python 2 is no longer supported, but may work.

Beginning with isarn-sketches-spark 0.5.0, only spark >= 3.0 is supported,
due to substantial changes to Spark's API for user defined aggregation.
Some context on these changes is available in this Spark Summmit
[talk](https://databricks.com/session_na20/user-defined-aggregation-in-apache-spark-a-love-story).

Versions of Spark and Scala that are currently supported:

- spark 3.0, scala 2.12

If you are interested in a spark/scala build that is not listed above, please contact me and/or file an issue!

## How to use from the Spark CLI
Several Spark CLI tools accept the `--packages` argument, including spark-shell and pyspark.
Following is an example using `spark-shell`:
```bash
$ spark-shell --packages "org.isarnproject:isarn-sketches-spark_2.12:0.5.1-sp3.0"
```
Note that you need to explicitly include the scala version as part of the package name.

## Examples

### Sketch a numeric column (scala)
```scala
scala> import org.isarnproject.sketches.java.TDigest, org.isarnproject.sketches.spark.tdigest._, scala.util.Random._

scala> val data = spark.createDataFrame(Vector.fill(1000){(nextInt(10), nextGaussian)})
data: org.apache.spark.sql.DataFrame = [_1: int, _2: double]

scala> val udf = TDigestAggregator.udf[Double](compression = 0.2, maxDiscrete = 25)
udf: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedAggregator( ...

scala> val agg = data.agg(udf($"_1"), udf($"_2")).first
agg: org.apache.spark.sql.Row = [TDigest(0.0 -> (105.0, 105.0), ...

scala> val (td1, td2) = (agg.getAs[TDigest](0), agg.getAs[TDigest](1))
td1: org.isarnproject.sketches.java.TDigest = TDigest(0.0 -> (105.0, 105.0), ...

scala> (td1.cdf(2), td2.cdf(2))
res0: (Double, Double) = (0.2365,0.9682691795524728)

scala> (td1.samplePMF, td2.samplePDF)
res1: (Double, Double) = (8.0,-0.6712314520185372)
```

### Sketch a numeric array column (scala)
```scala
scala> import org.isarnproject.sketches.java.TDigest, org.isarnproject.sketches.spark.tdigest._, scala.util.Random._

scala> val data = spark.createDataFrame(Vector.fill(1000){(nextInt(10), Vector.fill(5){nextGaussian})})
data: org.apache.spark.sql.DataFrame = [_1: int, _2: array<double>]

scala> val udf1 = TDigestAggregator.udf[Int](maxDiscrete = 25)
udf1: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedAggregator( ...

scala> val udf2 = TDigestArrayAggregator.udf[Double](compression = 0.5)
udf2: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedAggregator( ...

scala> val agg = data.agg(udf1($"_1"), udf2($"_2")).first
agg: org.apache.spark.sql.Row = [TDigest(0.0 -> (104.0, 104.0), ...

scala> agg.getAs[TDigest](0).samplePMF
res0: Double = 9.0

scala> agg.getAs[Seq[TDigest]](1).map(_.samplePDF)
res1: Seq[Double] = ArrayBuffer(-0.40804832001013713, -0.5952280168647848, -0.4973297020191356, -0.9404857531406351, 2.347194542873795)
```

### Sketch a column of ML Vector (scala)
```scala
scala> import org.isarnproject.sketches.java.TDigest, org.isarnproject.sketches.spark.tdigest._, scala.util.Random._, org.apache.spark.ml.linalg.Vectors

scala> val data = spark.createDataFrame(Vector.fill(1000){(nextInt(10), Vectors.dense(nextGaussian,nextGaussian,nextGaussian))})
data: org.apache.spark.sql.DataFrame = [_1: int, _2: vector]

scala> val udf = TDigestMLVecAggregator.udf(compression = 0.5, maxDiscrete = 0)
udf: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedAggregator( ...

scala> val agg = data.agg(udf($"_2")).first
agg: org.apache.spark.sql.Row = [WrappedArray(TDigest(-3.881918499979969 -> (1.0, 1.0), ...

scala> val samples = agg.getAs[Seq[TDigest]](0).map(_.samplePDF)
samples: Seq[Double] = ArrayBuffer(0.28973374164214244, 0.4981749043377094, -0.8945453848202787)

scala> val medians = agg.getAs[Seq[TDigest]](0).map(_.cdfInverse(0.5))
medians: Seq[Double] = ArrayBuffer(0.03123637037282659, -0.07172360154570709, -0.04260955558310061)
```

### Sketch a column of MLLib Vector (scala)
```scala
scala> import org.isarnproject.sketches.java.TDigest, org.isarnproject.sketches.spark.tdigest._, scala.util.Random._, org.apache.spark.mllib.linalg.Vectors

scala> val data = spark.createDataFrame(Vector.fill(1000){(nextInt(10), Vectors.dense(nextGaussian,nextGaussian,nextGaussian))})
data: org.apache.spark.sql.DataFrame = [_1: int, _2: vector]

scala> val udf = TDigestMLLibVecAggregator.udf(compression = 0.5, maxDiscrete = 0)
udf: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedAggregator( ...

scala> val agg = data.agg(udf($"_2")).first
agg: org.apache.spark.sql.Row = [WrappedArray(TDigest(-3.7272857909819344 -> (1.0, 1.0), ...

scala> val samples = agg.getAs[Seq[TDigest]](0).map(_.samplePDF)
samples: Seq[Double] = ArrayBuffer(0.8780228679691738, -0.7636457587390327, 2.213341479782084)

scala> val medians = agg.getAs[Seq[TDigest]](0).map(_.cdfInverse(0.5))
medians: Seq[Double] = ArrayBuffer(-0.01676307618586101, 0.03846529110807051, -0.029124197911563777)
```

### Reduce a column (or grouping) of T-Digests (scala)
```scala
scala> import org.isarnproject.sketches.java.TDigest, org.isarnproject.sketches.spark.tdigest._, scala.util.Random._

scala> val data = spark.createDataFrame(Vector.fill(5000){(nextInt(5), nextGaussian)}).toDF("g", "x")
data: org.apache.spark.sql.DataFrame = [g: int, x: double]

scala> val udf = TDigestAggregator.udf[Double]()
udf: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedAggregator( ...

scala> val grp = data.groupBy("g").agg(udf($"x").alias("tdigests"))
grp: org.apache.spark.sql.DataFrame = [g: int, tdigests: tdigest]

scala> grp.show()
+---+--------------------+                                                      
|  g|            tdigests|
+---+--------------------+
|  1|TDigest(-3.054140...|
|  3|TDigest(-3.368392...|
|  4|TDigest(-3.439268...|
|  2|TDigest(-3.927057...|
|  0|TDigest(-3.169235...|
+---+--------------------+

scala> val udfred = TDigestReduceAggregator.udf(compression = 0.7)
udfred: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedAggregator( ...

scala> val agg = grp.agg(udfred($"tdigests")).first
agg: org.apache.spark.sql.Row = [TDigest(-3.9270575018252663 -> (1.0, 1.0), ...

scala> val sample = agg.getAs[TDigest](0).sample
sample: Double = 0.6633542575218054
```

### Reduce a column (or grouping) of T-Digest Arrays (scala)
```scala
scala> import org.isarnproject.sketches.java.TDigest, org.isarnproject.sketches.spark.tdigest._, scala.util.Random._

scala> val data = spark.createDataFrame(Vector.fill(5000){(nextInt(5), Vector.fill(3) { nextGaussian })}).toDF("g", "x")
data: org.apache.spark.sql.DataFrame = [g: int, x: array<double>]

scala> val udf = TDigestArrayAggregator.udf[Double]()
udf: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedAggregator( ...

scala> val grp = data.groupBy("g").agg(udf($"x").alias("tdigests"))
grp: org.apache.spark.sql.DataFrame = [g: int, tdigests: array<tdigest>]

scala> grp.show()
+---+--------------------+                                                      
|  g|            tdigests|
+---+--------------------+
|  1|[TDigest(-3.25266...|
|  3|[TDigest(-3.05690...|
|  4|[TDigest(-3.66651...|
|  2|[TDigest(-3.46231...|
|  0|[TDigest(-2.94039...|
+---+--------------------+

scala> val udfred = TDigestArrayReduceAggregator.udf(compression = 0.7)
udfred: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedAggregator( ...

scala> val agg = grp.agg(udfred($"tdigests")).first
agg: org.apache.spark.sql.Row = [WrappedArray(TDigest(-3.6665115390677423 -> (1.0, 1.0), ...

scala> val samples = agg.getAs[Seq[TDigest]](0).map(_.sample)
samples: Seq[Double] = ArrayBuffer(-0.741335878221013, 0.981730493526761, -0.6359834079354106)
```

### Sketch a numeric column (python)
```python
>>> from random import gauss, randint
>>> from isarnproject.sketches.spark.tdigest import *
>>> data = spark.createDataFrame([[randint(1,10),gauss(0,1)] for x in range(1000)])
>>> udf1 = tdigestIntUDF("_1", maxDiscrete = 25)
>>> udf2 = tdigestDoubleUDF("_2", compression = 0.5)
>>> agg = data.agg(udf1, udf2).first()
>>> agg[0].samplePMF()
2.0
>>> agg[1].samplePDF()
-0.8707704090068431
```

### Sketch a numeric array column (python)
```python
>>> from random import gauss, randint
>>> from isarnproject.sketches.spark.tdigest import *
>>> data = spark.createDataFrame([[[gauss(0,1),gauss(0,1),gauss(0,1)]] for x in range(1000)])
>>> udf = tdigestDoubleArrayUDF("_1", compression = 0.7)
>>> agg = data.agg(udf).first()
>>> [td.samplePDF() for td in agg[0]]
[0.6802628605487977, -0.649936837383734, -0.84228662547744]
```

### Sketch a column of ML Vectors (python)
```python
>>> from random import gauss, randint
>>> from isarnproject.sketches.spark.tdigest import *
>>> from pyspark.ml.linalg import Vectors
>>> data = spark.createDataFrame([[Vectors.dense([gauss(0,1),gauss(0,1),gauss(0,1)])] for x in range(1000)])
>>> udf = tdigestMLVecUDF("_1", compression = 0.7)
>>> agg = data.agg(udf).first()
>>> [td.cdfInverse(0.5) for td in agg[0]]
[-0.03088430803668949, -0.002903353148573491, 0.01640559766046329]
```

### Sketch a column of MLLib Vectors (python)
```python
>>> from random import gauss, randint
>>> from isarnproject.sketches.spark.tdigest import *
>>> from pyspark.mllib.linalg import Vectors
>>> data = spark.createDataFrame([[Vectors.dense([gauss(0,1),gauss(0,1),gauss(0,1)])] for x in range(1000)])
>>> udf = tdigestMLLibVecUDF("_1", compression = 0.7)
>>> agg = data.agg(udf).first()
>>> [td.cdfInverse(0.5) for td in agg[0]]                                       
[-0.03390700592837903, -0.0479047778031452, -0.02260427238692181]
```

### Reduce a column (or grouping) of T-Digests (python)
```python
>>> from random import gauss, randint
>>> from isarnproject.sketches.spark.tdigest import *
>>> data = spark.createDataFrame([[randint(1,5), gauss(0,1)] for x in range(5000)]).toDF("g","x")
>>> grp = data.groupBy("g").agg(tdigestDoubleUDF("x").alias("tdigests"))
>>> grp.show()
+---+--------------------+
|  g|            tdigests|
+---+--------------------+
|  5|TDigest(-2.907724...|
|  1|TDigest(-2.914628...|
|  3|TDigest(-3.288239...|
|  2|TDigest(-3.389084...|
|  4|TDigest(-3.507597...|
+---+--------------------+

>>> udf = tdigestReduceUDF("tdigests")
>>> agg = grp.agg(udf).first()
>>> agg[0].sample()                                                             
-0.14793866496592997
```

### Reduce a column (or grouping) of T-Digest Arrays (python)
```python
>>> from random import gauss, randint
>>> from isarnproject.sketches.spark.tdigest import *
>>> data = spark.createDataFrame([[randint(1,5), [gauss(0,1),gauss(0,1),gauss(0,1)]] for x in range(5000)]).toDF("g","x")
>>> grp = data.groupBy("g").agg(tdigestDoubleArrayUDF("x").alias("tdigests"))
>>> grp.show()
+---+--------------------+                                                      
|  g|            tdigests|
+---+--------------------+
|  5|[TDigest(-3.38098...|
|  1|[TDigest(-2.88380...|
|  3|[TDigest(-3.40987...|
|  2|[TDigest(-3.75224...|
|  4|[TDigest(-2.66571...|
+---+--------------------+

>>> udf = tdigestArrayReduceUDF("tdigests")
>>> agg = grp.agg(udf).first()
>>> [td.cdfInverse(0.5) for td in agg[0]]                                       
[-0.04635615835441749, -0.025723034166600753, -0.025168480174964893]
```

### Compute feature importance with respect to a predictive model
```scala
scala> import org.isarnproject.pipelines.spark.fi.{TDigestFI,TDigestFIModel}, org.apache.spark.ml.regression.LinearRegression

scala> val training = spark.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
training: org.apache.spark.sql.DataFrame = [label: double, features: vector]    

scala> val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
lr: org.apache.spark.ml.regression.LinearRegression = linReg_5d7a1cf3dafa

scala> val lrModel = lr.fit(training)
lrModel: org.apache.spark.ml.regression.LinearRegressionModel = LinearRegressionModel: uid=linReg_5d7a1cf3dafa, numFeatures=10

scala> val fi = new TDigestFI().setCompression(0.3).setMaxDiscrete(10)
fi: org.isarnproject.pipelines.spark.fi.TDigestFI = TDigestFI_6837561844f2

scala> val fiMod = fi.fit(training).setTargetModel(lrModel).setDeviationMeasure("rms-dev").setFeatureNames(Array.tabulate(10){j=>s"x$j"})
fiMod: org.isarnproject.pipelines.spark.fi.TDigestFIModel = TDigestFI_6837561844f2

scala> val imp = fiMod.transform(training)
imp: org.apache.spark.sql.DataFrame = [name: string, importance: double]

scala> imp.show
+----+-------------------+
|name|         importance|
+----+-------------------+
|  x0|                0.0|
|  x1| 0.2642731504552658|
|  x2| 0.2775267570310568|
|  x3|   1.48027354456237|
|  x4| 0.0442095774509019|
|  x5|  0.620636336433091|
|  x6|                0.0|
|  x7|0.12650113005096197|
|  x8| 0.1644528333598182|
|  x9| 0.1883875750326046|
+----+-------------------+
```

### Compute feature importance with respect to a predictive model (python)
```python
>>> from isarnproject.pipelines.spark.fi import *
>>> from pyspark.ml.regression import LinearRegression
>>> training = spark.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
>>> lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)        
>>> lrModel = lr.fit(training)
>>> fi = TDigestFI(compression = 0.3, maxDiscrete = 10)
>>> fiMod = fi.fit(training).setTargetModel(lrModel).setDeviationMeasure("rms-dev").setFeatureNames(["x%d" % (j) for j in range(10)])
>>> imp = fiMod.transform(training)
>>> imp.show()
+----+--------------------+
|name|          importance|
+----+--------------------+
|  x0|                 0.0|
|  x1|  0.2617304778862077|
|  x2| 0.26451433792352613|
|  x3|  1.5244246022297059|
|  x4|0.043227915487816015|
|  x5|  0.6195605571925815|
|  x6|                 0.0|
|  x7| 0.11735009989902982|
|  x8| 0.17250227692634765|
|  x9| 0.18251143533748138|
+----+--------------------+
```
