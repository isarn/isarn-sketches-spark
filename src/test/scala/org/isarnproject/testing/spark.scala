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

package org.isarnproject.testing.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import utest._

abstract class SparkTestSuite extends TestSuite {
  private lazy val sparkConf: SparkConf = {
    val conf = new SparkConf()
      .setAppName("SparkTestSuite")
    conf
  }

  private lazy val sparkSession: SparkSession = {
    val ss = SparkSession.builder
      .master("local[2]")
      .config(sparkConf)
      .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    ss
  }

  def spark: SparkSession = sparkSession

  override def utestAfterAll(): Unit = {
    super.utestAfterAll()
    spark.stop()
  }
}
