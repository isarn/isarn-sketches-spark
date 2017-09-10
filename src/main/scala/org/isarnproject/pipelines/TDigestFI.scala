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

//import scala.language.existentials
import scala.reflect.ClassTag

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
    private val featTD: Array[TDigest],
    private val predModel: M,
    private val spark: SparkSession
  )(implicit ctM: ClassTag[M]) extends Model[TDigestFIModel[M]] with TDigestFIParams {

  private val featTDBC = spark.sparkContext.broadcast(featTD)
  private val predModelBC = spark.sparkContext.broadcast(predModel)

  private val predictMethod =
    predModel.getClass.getDeclaredMethods.find(_.getName == "predict").get

  override def copy(extra: ParamMap): TDigestFIModel[M] = ???

  def transformSchema(schema: StructType): StructType =
    this.validateAndTransformSchema(schema)

  def transform(data: Dataset[_]): DataFrame = {
    transformSchema(data.schema, logging = true)
    val udaf = new TDigestFIUDAF(featTDBC, predModelBC, (x1: Double, x2: Double) => math.abs(x1-x2))
    val ti = data.agg(udaf(col($(featuresCol))))
    val importances = ti.first.getAs[Array[Double]](0)
    val names = (1 to importances.length).map { j => s"f$j" }
    spark.createDataFrame(names.zip(importances)).toDF($(nameCol), $(importanceCol))
  }

  override def finalize(): Unit = {
    featTDBC.unpersist
    predModelBC.unpersist
    super.finalize()
  }
}

class TDigestFIEstimator[M <: PredictionModel[MLVector, M]](
    override val uid: String,
    private val predModel: M
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

  private val predictMethod = {
    val pm = predModel.value.getClass.getDeclaredMethods.find(_.getName == "predict").get
    pm.setAccessible(true)
    pm
  }

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
    buf(0) = Array.fill(m)(0.0)
    buf(1) = 0L
  }

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    val ftd = featTD.value
    val model = predModel.value
    val dev = buf.getAs[Array[Double]](0)
    val n = buf.getAs[Long](1)
    val farr = input.getAs[MLVector](0).toArray
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
    val dev1 = buf1.getAs[Array[Double]](0)
    val dev2 = buf2.getAs[Array[Double]](0)
    val n1 = buf1.getAs[Long](1)
    val n2 = buf2.getAs[Long](1)    
    for { j <- 0 until m } {
      dev1(j) += dev2(j)
    }
    buf1(0) = dev1
    buf1(1) = n1 + n2
  }

  def evaluate(buf: Row): Any = {
    val dev = buf.getAs[Array[Double]](0)
    val n = buf.getAs[Long](1).toDouble
    for { j <- 0 until m } {
      dev(j) /= n
    }
    dev
  }
}
