/*
Copyright 2017 Erik Erlandson
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.isarnproject.pipelines

import scala.reflect.ClassTag
import scala.collection.mutable.WrappedArray

import org.apache.spark.ml.{Estimator, Model, PredictionModel}
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.regression.RegressionModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{Vector=>MLVector, DenseVector => MLDense}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row

import org.isarnproject.sketches.TDigest
import org.apache.spark.isarnproject.sketches.udt._
import org.isarnproject.sketches.udaf._

private[pipelines] trait TDigestFIParams extends Params with DefaultParamsWritable {

  final val delta: DoubleParam =
    new DoubleParam(this, "delta", "t-digest compression (> 0)", ParamValidators.gt(0.0))
  setDefault(delta, org.isarnproject.sketches.TDigest.deltaDefault)
  final def getDelta: Double = $(delta)
  final def setDelta(value: Double): this.type = set(delta, value)

  final val maxDiscrete: IntParam =
    new IntParam(this, "maxDiscrete", "maximum unique discrete values (>= 0)", ParamValidators.gtEq(0))
  setDefault(maxDiscrete, 0)
  final def getMaxDiscrete: Int = $(maxDiscrete)
  final def setMaxDiscrete(value: Int): this.type = set(maxDiscrete, value)

  final val featuresCol: Param[String] = new Param[String](this, "featuresCol", "feature column name")
  setDefault(featuresCol, "features")
  final def getFeaturesCol: String = $(featuresCol)
  final def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  final val nameCol: Param[String] =
    new Param[String](this, "nameCol", "column for names of features")
  setDefault(nameCol, "name")
  final def getNameCol: String = $(nameCol)
  final def setNameCol(value: String): this.type = set(nameCol, value)

  final val importanceCol: Param[String] =
    new Param[String](this, "importance", "column for feature importance values")
  setDefault(importanceCol, "importance")
  final def getImportanceCol: String = $(importanceCol)
  final def setImportanceCol(value: String): this.type = set(importanceCol, value)

  final val deviationMeasure: Param[String] =
    new Param[String](this, "deviationMeasure", "deviation measure to apply")
  setDefault(deviationMeasure, "auto")
  final def getDeviationMeasure: String = $(deviationMeasure)
  final def setDeviationMeasure(value: String): this.type = set(deviationMeasure, value)

  final val featureNames: StringArrayParam =
    new StringArrayParam(this, "featureNames", "assume these feature names")
  setDefault(featureNames, Array.empty[String])
  final def getFeatureNames: Array[String] = $(featureNames)
  final def setFeatureNames(value: Array[String]): this.type = set(featureNames, value)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    // we want the input column to exist...
    require(schema.fieldNames.contains($(featuresCol)))

    // ...and to be the proper type
    schema($(featuresCol)) match {
      case sf: StructField => require(sf.dataType.equals(TDigestUDTInfra.udtVectorML))
    }

    // output is two columns: feature names and corresponding importances
    StructType(Seq(
      StructField($(nameCol), StringType, false),
      StructField($(importanceCol), DoubleType, false)
    ))
  }
}

class TDigestFIModel[M <: PredictionModel[MLVector, M]](
    override val uid: String,
    featTD: Array[TDigest],
    predModel: M,
    spark: SparkSession
  )(implicit ctM: ClassTag[M]) extends Model[TDigestFIModel[M]] with TDigestFIParams {

  private val featTDBC = spark.sparkContext.broadcast(featTD)
  private val predModelBC = spark.sparkContext.broadcast(predModel)

  private val deviation: (Double, Double) => Double = $(deviationMeasure) match {
    case "mean-abs-dev" => (x1: Double, x2: Double) => math.abs(x1 - x2)
    case "rms-dev" => (x1: Double, x2: Double) => math.pow(x1 - x2, 2)
    case "dev-rate" => (x1: Double, x2: Double) => if (x1 != x2) 1.0 else 0.0
    case "auto" => {
      inheritances(predModel) match {
        case ih if ih.contains("RegressionModel") => (x1: Double, x2: Double) => math.abs(x1 - x2)
        case ih if ih.contains("ClassificationModel") => (x1: Double, x2: Double) => if (x1 != x2) 1.0 else 0.0
        case _ => throw new Exception(s"bad model class ${predModel.getClass.getSimpleName}")
      }
    }
    case _ => throw new Exception(s"bad deviation measure ${this.getDeviationMeasure}")
  }

  override def copy(extra: ParamMap): TDigestFIModel[M] = ???

  def transformSchema(schema: StructType): StructType =
    this.validateAndTransformSchema(schema)

  def transform(data: Dataset[_]): DataFrame = {
    transformSchema(data.schema, logging = true)
    val udaf = new TDigestFIUDAF(featTDBC, predModelBC, deviation)
    val ti = data.agg(udaf(col($(featuresCol))))
    val imp = ti.first.get(0).asInstanceOf[WrappedArray[Double]]
    val importances = if ($(deviationMeasure) != "rms-dev") imp else imp.map { x => math.sqrt(x) }
    val featNames: Seq[String] =
      if ($(featureNames).length > 0) $(featureNames) else (1 to featTD.length).map { j => s"f$j" }    
    require(featNames.length == featTD.length, s"expecting ${featTD.length} feature names")
    spark.createDataFrame(featNames.zip(importances)).toDF($(nameCol), $(importanceCol))
  }

  override def finalize(): Unit = {
    featTDBC.unpersist
    predModelBC.unpersist
    super.finalize()
  }
}

class TDigestFI[M <: PredictionModel[MLVector, M]](
    override val uid: String,
    predModel: M
  )(implicit ctM: ClassTag[M]) extends Estimator[TDigestFIModel[M]] with TDigestFIParams {

  def this(pm: M)(implicit ctM: ClassTag[M]) =
    this(Identifiable.randomUID("TDigestFI"), pm)

  override def copy(extra: ParamMap): Estimator[TDigestFIModel[M]] = ???

  def transformSchema(schema: StructType): StructType =
    this.validateAndTransformSchema(schema)

  def fit(data: Dataset[_]): TDigestFIModel[M] = {
    transformSchema(data.schema, logging = true)
    val udaf = tdigestMLVecUDAF.delta($(delta)).maxDiscrete($(maxDiscrete))
    val agg = data.agg(udaf(col($(featuresCol))))
    val tds = agg.first.getAs[TDigestArraySQL](0).tdigests
    val model = new TDigestFIModel(uid, tds, predModel, data.sparkSession)
    model.setParent(this)
    model
  }
}

class TDigestFIUDAF[M <: PredictionModel[MLVector, M]](
    featTD: Broadcast[Array[TDigest]],
    predModel: Broadcast[M],
    deviation: (Double, Double) => Double
  ) extends UserDefinedAggregateFunction {

  private val m = featTD.value.length

  def deterministic: Boolean = false

  def inputSchema: StructType =
    StructType(StructField("features", TDigestUDTInfra.udtVectorML, false) :: Nil)

  def dataType: DataType = ArrayType(DoubleType, false)

  def bufferSchema: StructType =
    StructType(
      StructField("dev", ArrayType(DoubleType, false), false) ::
      StructField("n", LongType, false) ::
      Nil)

  def initialize(buf: MutableAggregationBuffer): Unit = {
    buf(0) =  WrappedArray.make[Double](Array.fill(m)(0.0))
    buf(1) = 0L
  }

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    val ftd = featTD.value
    val model = predModel.value
    // The 'predict' method is part of the generic PredictionModel interface,
    // however it is protected, so I have to force the issue using reflection.
    // Method is not serializable, so I have to do it inside the update function each time.
    val predictMethod = model.getClass.getDeclaredMethods.find(_.getName == "predict").get
    predictMethod.setAccessible(true)
    val dev = buf.getAs[WrappedArray[Double]](0)
    val n = buf.getAs[Long](1)
    val farr = input.getAs[MLVector](0).toArray
    require(farr.length == m, "bad feature vector length ${farr.length}")
    // Declaring a dense vector around 'farr' allows me to overwrite individual
    // feature values below. This works because dense vec just wraps the underlying
    // array value. If the implementation of dense vec changes, this could break,
    // although it seems unlikely.
    val fvec = (new MLDense(farr)).asInstanceOf[AnyRef]
    val refpred = predictMethod.invoke(model, fvec).asInstanceOf[Double]
    for { j <- 0 until m } {
      val t = farr(j)
      farr(j) = ftd(j).sample
      val pred = predictMethod.invoke(model, fvec).asInstanceOf[Double]
      farr(j) = t
      dev(j) += deviation(refpred, pred)
    }
    buf(0) = dev
    buf(1) = n + 1
  }

  def merge(buf1: MutableAggregationBuffer, buf2: Row): Unit = {
    val dev1 = buf1.getAs[WrappedArray[Double]](0)
    val dev2 = buf2.getAs[WrappedArray[Double]](0)
    val n1 = buf1.getAs[Long](1)
    val n2 = buf2.getAs[Long](1)    
    for { j <- 0 until m } {
      dev1(j) += dev2(j)
    }
    buf1(0) = dev1
    buf1(1) = n1 + n2
  }

  def evaluate(buf: Row): Any = {
    val dev = buf.getAs[WrappedArray[Double]](0)
    val n = buf.getAs[Long](1).toDouble
    for { j <- 0 until m } {
      dev(j) /= n
    }
    dev
  }
}

object test {
  import scala.util.Random
  import org.apache.spark.ml.regression.LinearRegression
  def apply(spark: SparkSession) = {
    new AnyRef {
      val raw = Vector.fill(1000) { Array.fill(3) { Random.nextGaussian() } }
      val rawlab = raw.map { v => 3 * v(0) + 5 * v(1) - 7 * v(2) + 11 }
      val data = spark.createDataFrame(raw.map { v => new MLDense(v) }.zip(rawlab)).toDF("features", "label")
      val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      val lrModel = lr.fit(data)
      val fi = new TDigestFI(lrModel)
      val fiModel = fi.fit(data)
      val imp = fiModel.transform(data)
    }
  }
}
