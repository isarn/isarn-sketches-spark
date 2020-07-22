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

package org.isarnproject.sketches.spark.tdigest

import scala.util.Random._

import utest._

import org.isarnproject.testing.spark.SparkTestSuite
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{ Vectors => VectorsML }
import org.apache.spark.mllib.linalg.{ Vectors => VectorsMLLib }

import org.isarnproject.sketches.java.TDigest

object TDigestAggregationSuite extends SparkTestSuite {

  import CDFFunctions._

  // set the seed before generating any data
  setSeed(7337L * 3773L)

  // don't use lazy values because then data generation order may be undefined,
  // due to test execution order
  val data1 = spark.createDataFrame(Vector.fill(10001){(nextInt(10), nextGaussian)})
    .toDF("j","x")
    .cache()

  val data2 = spark.createDataFrame(Vector.fill(10002){(nextInt(10), Vector.fill(3){nextGaussian})})
    .toDF("j", "x")
    .cache()

  val data3 = spark.createDataFrame(Vector.fill(10003){(nextInt(10), VectorsML.dense(nextGaussian,nextGaussian,nextGaussian))})
    .toDF("j", "x")
    .cache()

  val data4 = spark.createDataFrame(Vector.fill(10004){(nextInt(10), VectorsMLLib.dense(nextGaussian,nextGaussian,nextGaussian))})
    .toDF("j", "x")
    .cache()

  // Spark DataFrames and RDDs are lazy.
  // Make sure data are actually created prior to testing, or ordering
  // may change based on test ordering
  val count1 = data1.count()
  val count2 = data2.count()
  val count3 = data3.count()
  val count4 = data4.count()

  val epsD = 0.02

  val tests = Tests {
    test("TDigestAggregator") {
      assert(data1.rdd.partitions.size > 1)
      val udf = TDigestAggregator.udf[Double](compression = 0.25, maxDiscrete = 25)
      val agg = data1.agg(udf(col("j")), udf(col("x"))).first
      val (tdj, tdx) = (agg.getAs[TDigest](0), agg.getAs[TDigest](1))
      approx(tdj.mass(), count1)
      approx(tdx.mass(), count1)
      assert(KSD(tdj, discreteUniformCDF(0, 9)) < epsD)
      assert(KSD(tdx, gaussianCDF(0,1)) < epsD)
    }

    test("TDigestArrayAggregator") {
      assert(data2.rdd.partitions.size > 1)
      val udfj = TDigestAggregator.udf[Int](maxDiscrete = 25)
      val udfx = TDigestArrayAggregator.udf[Double](compression = 0.25)
      val agg = data2.agg(udfj(col("j")), udfx(col("x"))).first
      val (tdj, tdx) = (agg.getAs[TDigest](0), agg.getAs[Seq[TDigest]](1))
      approx(tdj.mass(), count2)
      assert(KSD(tdj, discreteUniformCDF(0, 9)) < epsD)
      for { td <- tdx } {
        approx(td.mass(), count2)
        assert(KSD(td, gaussianCDF(0, 1)) < epsD)
      }
    }

    test("TDigestMLVecAggregator") {
      assert(data3.rdd.partitions.size > 1)
      val udfj = TDigestAggregator.udf[Int](maxDiscrete = 25)
      val udfx = TDigestMLVecAggregator.udf(compression = 0.25)
      val agg = data3.agg(udfj(col("j")), udfx(col("x"))).first
      val (tdj, tdx) = (agg.getAs[TDigest](0), agg.getAs[Seq[TDigest]](1))
      approx(tdj.mass(), count3)
      assert(KSD(tdj, discreteUniformCDF(0, 9)) < epsD)
      for { td <- tdx } {
        approx(td.mass(), count3)
        assert(KSD(td, gaussianCDF(0, 1)) < epsD)
      }
    }

    test("TDigestMLLibVecAggregator") {
      assert(data4.rdd.partitions.size > 1)
      val udfj = TDigestAggregator.udf[Int](maxDiscrete = 25)
      val udfx = TDigestMLLibVecAggregator.udf(compression = 0.25)
      val agg = data4.agg(udfj(col("j")), udfx(col("x"))).first
      val (tdj, tdx) = (agg.getAs[TDigest](0), agg.getAs[Seq[TDigest]](1))
      approx(tdj.mass(), count4)
      assert(KSD(tdj, discreteUniformCDF(0, 9)) < epsD)
      for { td <- tdx } {
        approx(td.mass(), count4)
        assert(KSD(td, gaussianCDF(0, 1)) < epsD)
      }
    }
  }

  def approx(x: Double, t: Double, eps: Double = 1e-4): Unit =
    assert(math.abs(x - t) < eps)
}

object CDFFunctions {
  type CDF = Double => Double

  // Kolmogorov Smirnov D-statistic
  def KSD(td: TDigest, cdf: CDF, n: Int = 1000): Double = {
    require(td.size() > 1)
    require(n > 0)
    val xmin = td.cdfInverse(0)
    val xmax = td.cdfInverse(1)
    val step = (xmax - xmin) / n.toDouble
    val tdcdf = if (td.size() <= td.getMaxDiscrete()) td.cdfDiscrete(_) else td.cdf(_)
    (BigDecimal(xmin) until xmax by step)
      .iterator
      .map(_.toDouble)
      .map(x => math.abs(tdcdf(x) - cdf(x)))
      .max
  }

  def gaussianCDF(mean: Double = 0, stdv: Double = 1): CDF = {
    require(stdv > 0.0)
    val z = stdv * math.sqrt(2.0)
    (x: Double) => (1.0 + erf((x - mean) / z)) / 2.0
  }

  def discreteUniformCDF(xmin: Int, xmax: Int): CDF = {
    require(xmax > xmin)
    require(xmin >= 0)
    val z = (1 + xmax - xmin).toDouble
    (x: Double) => {
      if (x < xmin.toDouble) 0.0 else if (x >= xmax.toDouble) 1.0 else {
        val xi = x.toInt
        (1 + xi - xmin).toDouble / z
      }
    }
  }

  // https://en.wikipedia.org/wiki/Error_function#Approximation_with_elementary_functions
  def erf(x: Double): Double = {
    // erf is an odd function
    if (x < 0.0) -erf(-x) else {
      val t = 1.0 / (1.0 + (0.47047 * x))
      var u = t
      var s = 0.0
      s += 0.3480242 * u
      u *= t
      s -= 0.0958798 * u
      u *= t
      s += 0.7478556 * u
      s *= math.exp(-(x * x))
      1.0 - s
    }
  }
}
