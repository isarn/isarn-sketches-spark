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

package org.isarnproject.pipelines.spark.fi

import scala.util.Random._

import utest._

import org.isarnproject.testing.spark.SparkTestSuite

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors

object FeatureImportanceSuite extends SparkTestSuite {

  // set the seed before generating any data
  setSeed(7337L * 3773L)

  val raw1 = Vector.fill(1000){ Vectors.dense(nextGaussian, nextGaussian, nextGaussian) }
    .map{v => (5*v(0) + 2*v(2), v)}
  val train1 = spark.createDataFrame(raw1).toDF("label", "features").cache()

  // Spark DataFrames and RDDs are lazy.
  // Make sure data are actually created prior to testing, or ordering
  // may change based on test ordering
  val count1 = train1.count()

  val tests = Tests {
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val lrModel = lr.fit(train1)

    val fi = new TDigestFI().setCompression(0.3)
    val fiMod = fi.fit(train1)
      .setTargetModel(lrModel)
      .setDeviationMeasure("rms-dev")
      .setFeatureNames(Array.tabulate(3){j=>s"x$j"})

    val imp = fiMod.transform(train1)
    val impmap = imp.collect.map { r =>
      (r.getAs[String](0), r.getAs[Double](1)) }
      .toMap

    approx(impmap("x0"), 6.65, 0.5)
    approx(impmap("x1"), 0.00, 0.001)
    approx(impmap("x2"), 2.50, 0.5)
  }
}
