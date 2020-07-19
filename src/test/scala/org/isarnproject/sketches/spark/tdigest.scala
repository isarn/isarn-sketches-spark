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

import org.isarnproject.sketches.java.TDigest

object TDigestAggregationSuite extends SparkTestSuite {

  // set the seed before generating any data
  setSeed(7337L * 3773L)
  // don't use lazy values because then data generation order may be undefined,
  // due to test execution order
  val data1 = spark.createDataFrame(Vector.fill(10000){(nextInt(10), nextGaussian)})
    .toDF("j","x")
    .cache()

  // make sure data are actually created prior to testing
  data1.count()

  val tests = Tests {
    test("TDigestAggregator") {
      assert(data1.rdd.partitions.size > 1)
      val udf = TDigestAggregator.udf[Double](compression = 0.2, maxDiscrete = 25)
      val agg = data1.agg(udf(col("j")), udf(col("x"))).first
      val (tdj, tdx) = (agg.getAs[TDigest](0), agg.getAs[TDigest](1))
      approx(tdj.cdf(2), 0.20, eps = 0.05)
      approx(tdx.cdf(2), 0.97, eps = 0.05)
    }
  }

  def approx(x: Double, t: Double, eps: Double = 1e-4): Unit =
    assert(math.abs(x - t) < eps)
}

