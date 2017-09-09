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

import org.apache.spark.ml.{Estimator, Model, PredictionModel}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, DataFrame}

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

class TDigestFIModel(
    override val uid: String,
    private val model: Array[TDigest],
    private val predModel: PredictionModel[_ , _ <: PredictionModel[_, _]]
  ) extends Model[TDigestFIModel] with TDigestFIParams {

  override def copy(extra: ParamMap): TDigestFIModel = ???

  def transformSchema(schema: StructType): StructType =
    this.validateAndTransformSchema(schema)

  def transform(data: Dataset[_]): DataFrame = ???
}

class TDigestFIEstimator(
    override val uid: String,
    private val predModel: PredictionModel[_ , _ <: PredictionModel[_, _]]
  ) extends Estimator[TDigestFIModel] with TDigestFIParams {

  def this(pm: PredictionModel[_ , _ <: PredictionModel[_, _]]) =
    this(Identifiable.randomUID("TDigestFI"), pm)

  override def copy(extra: ParamMap): Estimator[TDigestFIModel] = ???

  def transformSchema(schema: StructType): StructType =
    this.validateAndTransformSchema(schema)

  def fit(data: Dataset[_]): TDigestFIModel = {
    transformSchema(data.schema, logging = true)
    val udaf = tdigestMLVecUDAF.delta($(delta)).maxDiscrete($(maxDiscrete))
    val agg = data.agg(udaf(col($(featuresCol))))
    val tds = agg.first.getAs[TDigestArraySQL](0).tdigests
    new TDigestFIModel(uid, tds, predModel)
  }
}
