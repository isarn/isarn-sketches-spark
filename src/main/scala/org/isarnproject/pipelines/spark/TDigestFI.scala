/*
Copyright 2017-2020 Erik Erlandson
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
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.regression.RegressionModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{Vector => MLVector,
  DenseVector => MLDense, SparseVector => MLSparse}
import org.apache.spark.sql.Row

import org.isarnproject.sketches.java.TDigest
import org.apache.spark.isarnproject.sketches.tdigest.udt.infra.udtVectorML

// Defining these in a subpackage so the package can have other
// param definitions added to it elsewhere. I'm keeping them visible
// so other packages can use them in the future if there is a use
package params {
  trait HasFeaturesCol extends Params with DefaultParamsWritable {

    /**
     * Column containing feature vectors. Expected type is ML Vector.
     * Defaults to "features"
     * @group param
     */
    final val featuresCol: Param[String] =
      new Param[String](this, "featuresCol", "feature column name")
    setDefault(featuresCol, "features")
    final def getFeaturesCol: String = $(featuresCol)
    final def setFeaturesCol(value: String): this.type = set(featuresCol, value)
  }

  trait TDigestParams extends Params with DefaultParamsWritable {
    /**
     * TDigest compression parameter.
     * Defaults to 0.5
     * @group param
     */
    final val delta: DoubleParam =
      new DoubleParam(this, "delta", "t-digest compression (> 0)", ParamValidators.gt(0.0))
    setDefault(delta, org.isarnproject.sketches.TDigest.deltaDefault)
    final def getDelta: Double = $(delta)
    final def setDelta(value: Double): this.type = set(delta, value)

    /**
     * Maximum number of discrete values to sketch before transitioning to continuous mode
     * Defaults to 0
     * @group param
     */
    final val maxDiscrete: IntParam =
      new IntParam(this, "maxDiscrete", "maximum unique discrete values (>= 0)",
        ParamValidators.gtEq(0))
    setDefault(maxDiscrete, 0)
    final def getMaxDiscrete: Int = $(maxDiscrete)
    final def setMaxDiscrete(value: Int): this.type = set(maxDiscrete, value)
  }

  trait TDigestFIParams extends Params with TDigestParams with HasFeaturesCol

  trait TDigestFIModelParams extends Params
      with HasFeaturesCol with DefaultParamsWritable {

    /**
     * A predictive model to compute variable importances against.
     * No default.
     * @group param
     */
    final val targetModel: Param[AnyRef] =
      new Param[AnyRef](this, "targetModel", "predictive model")
    // no default for this
    final def getTargetModel: AnyRef = $(targetModel)
    final def setTargetModel(value: AnyRef): this.type = {
      if (!inheritances(value).contains("PredictionModel")) {
         throw new Exception("model must be a subclass of PredictionModel")
      }
      set(targetModel, value)
    }

    /**
     * Column name to use for feature names.
     * Defaults to "name"
     * @group param
     */
    final val nameCol: Param[String] =
      new Param[String](this, "nameCol", "column for names of features")
    setDefault(nameCol, "name")
    final def getNameCol: String = $(nameCol)
    final def setNameCol(value: String): this.type = set(nameCol, value)

    /**
     * Column name to use for feature importances.
     * Defaults to "importance"
     * @group param
     */
    final val importanceCol: Param[String] =
      new Param[String](this, "importanceCol", "column for feature importance values")
    setDefault(importanceCol, "importance")
    final def getImportanceCol: String = $(importanceCol)
    final def setImportanceCol(value: String): this.type = set(importanceCol, value)

    /**
     * Function to measure the change resulting from randomizing a feature value.
     * Defaults to "auto" (detects whether model is regression or classification).
     * Options: "auto", "dev-rate" (class), "abs-dev" (reg), "rms-dev" (reg)
     * @group param
     */
    final val deviationMeasure: Param[String] =
      new Param[String](this, "deviationMeasure", "deviation measure to apply")
    setDefault(deviationMeasure, "auto")
    final def getDeviationMeasure: String = $(deviationMeasure)
    final def setDeviationMeasure(value: String): this.type = set(deviationMeasure, value)

    /**
     * Names to use for features.
     * Defaults to f1, f2, ...
     * @group param
     */
    final val featureNames: StringArrayParam =
      new StringArrayParam(this, "featureNames", "assume these feature names")
    setDefault(featureNames, Array.empty[String])
    final def getFeatureNames: Array[String] = $(featureNames)
    final def setFeatureNames(value: Array[String]): this.type = set(featureNames, value)
  }
}

import params._

/**
 * Model/Transformer for transforming input feature data into a DataFrame containing
 * "name" and "importance" columns, mapping feature name to its computed importance.
 */
class TDigestFIModel(
    override val uid: String,
    featTD: Array[TDigest],
    spark: SparkSession
  ) extends Model[TDigestFIModel] with TDigestFIModelParams {

  private val featTDBC = spark.sparkContext.broadcast(featTD)

  private def deviation: (Double, Double) => Double = $(deviationMeasure) match {
    case "mean-abs-dev" => (x1: Double, x2: Double) => math.abs(x1 - x2)
    case "rms-dev" => (x1: Double, x2: Double) => math.pow(x1 - x2, 2)
    case "dev-rate" => (x1: Double, x2: Double) => if (x1 != x2) 1.0 else 0.0
    case "auto" => {
      inheritances($(targetModel)) match {
        case ih if ih.contains("RegressionModel") =>
          (x1: Double, x2: Double) => math.abs(x1 - x2)
        case ih if ih.contains("ClassificationModel") =>
          (x1: Double, x2: Double) => if (x1 != x2) 1.0 else 0.0
        case _ =>
          throw new Exception(s"bad model class ${this.getTargetModel.getClass.getSimpleName}")
      }
    }
    case _ => throw new Exception(s"bad deviation measure ${this.getDeviationMeasure}")
  }

  override def copy(extra: ParamMap): TDigestFIModel = ???

  def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains($(featuresCol)))
    schema($(featuresCol)) match {
      case sf: StructField => require(sf.dataType.equals(udtVectorML))
    }

    // Output is two columns: feature names and corresponding importances
    StructType(Seq(
      StructField($(nameCol), StringType, false),
      StructField($(importanceCol), DoubleType, false)
    ))
  }

  def transform(data: Dataset[_]): DataFrame = {
    transformSchema(data.schema, logging = true)
    val modelBC = spark.sparkContext.broadcast($(targetModel))
    val fdev = deviation
    val (n, imp) = data.select(col($(featuresCol))).rdd.mapPartitions { (fvp: Iterator[Row]) =>
      val ftd = featTDBC.value
      val model = modelBC.value
      // The 'predict' method is part of the generic PredictionModel interface,
      // however it is protected, so I have to force the issue using reflection.
      // 'Method' is not serializable, so I have to do it inside this map
      val predictMethod = model.getClass.getDeclaredMethods.find(_.getName == "predict").get
      predictMethod.setAccessible(true)
      val m = ftd.length
      val (n, dev) = fvp.foldLeft((0L, Array.fill(m)(0.0))) { case ((n, dev), Row(fv: MLVector)) =>
        val farr = fv.toArray
        require(farr.length == m, "bad feature vector length ${farr.length}")
        // Declaring a dense vector around 'farr' allows me to overwrite individual
        // feature values below. This works because dense vec just wraps the underlying
        // array value. If the implementation of dense vec changes, this could break,
        // although it seems unlikely.
        val fvec = (new MLDense(farr)).asInstanceOf[AnyRef]
        val refpred = predictMethod.invoke(model, fvec).asInstanceOf[Double]
        for { j <- 0 until m } {
          val t = farr(j)
          farr(j) = ftd(j).sample()
          val pred = predictMethod.invoke(model, fvec).asInstanceOf[Double]
          farr(j) = t
          dev(j) += fdev(refpred, pred)
        }
        (n + 1, dev)
      }
      Iterator((n, dev))
    }.treeReduce { case ((n1, dev1), (n2, dev2)) =>
      require(dev1.length == dev2.length, "mismatched deviation vector sizes")
      for { j <- 0 until dev1.length } { dev1(j) += dev2(j) }
      (n1 + n2, dev1)
    }
    val nD = n.toDouble
    for { j <- 0 until imp.length } { imp(j) /= nD }
    val importances = if ($(deviationMeasure) != "rms-dev") imp else imp.map { x => math.sqrt(x) }
    val featNames: Seq[String] =
      if ($(featureNames).length > 0) {
        $(featureNames)
      } else {
        (1 to featTD.length).map { j => s"f$j" }
      }
    require(featNames.length == featTD.length, s"expecting ${featTD.length} feature names")
    modelBC.unpersist
    spark.createDataFrame(featNames.zip(importances)).toDF($(nameCol), $(importanceCol))
  }

  override def finalize(): Unit = {
    featTDBC.unpersist
    super.finalize()
  }
}

/**
 * An Estimator for creating a TDigestFI model from feature data.
 */
class TDigestFI(override val uid: String) extends Estimator[TDigestFIModel] with TDigestFIParams {

  def this() = this(Identifiable.randomUID("TDigestFI"))

  override def copy(extra: ParamMap): Estimator[TDigestFIModel] = ???

  def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains($(featuresCol)))
    schema($(featuresCol)) match {
      case sf: StructField => require(sf.dataType.equals(udtVectorML))
    }
    // I can't figure out the purpose for outputting a modified schema here.
    // Until further notice I'm going to output an empty one.
    StructType(Seq.empty[StructField])
  }

  def fit(data: Dataset[_]): TDigestFIModel = {
    val tds = data.select(col($(featuresCol))).rdd
      .treeAggregate(Array.empty[TDigest])({ case (ttd, Row(fv: MLVector)) =>
        val m = fv.size
        val td =
          if (ttd.length > 0) ttd else Array.fill(m)(TDigest.empty($(delta), $(maxDiscrete)))
        require(td.length == m, "Inconsistent feature vector size $m")
        fv match {
          case v: MLSparse =>
            var jBeg = 0
            v.foreachActive((j, x) => {
              for { k <- jBeg until j } { td(k).update(0.0) }
              td(j).update(x)
              jBeg = j + 1
            })
            for { k <- jBeg until v.size } { td(k).update(0.0) }
          case _ =>
            for { j <- 0 until fv.size } { td(j).update(fv(j)) }
        }
        td
      },
      (td1, td2) =>
        if (td1.length == 0) {
          td2
        } else if (td2.length == 0) {
          td1
        } else {
          require(td1.length == td2.length, "mismatched t-digest arrays")
          for { j <- 0 until td1.length } {
            td1(j).merge(td2(j))
          }
          td1
        })
    val model = new TDigestFIModel(uid, tds, data.sparkSession)
    model.setParent(this)
    model
  }
}
