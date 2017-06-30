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

package org.isarnproject.sketches.udaf

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row

import org.isarnproject.sketches.TDigest

import org.apache.spark.isarnproject.sketches.udt._

case class TDigestUDAF(deltaV: Double, maxDiscreteV: Int) extends UserDefinedAggregateFunction {

  def delta(deltaP: Double) = this.copy(deltaV = deltaP)

  def maxDiscrete(maxDiscreteP: Int) = this.copy(maxDiscreteV = maxDiscreteP)

  // A t-digest is deterministic, but it is only statistically associative or commutative
  // and spark will merge partition results in nondeterministic order. That makes
  // the result of the aggregation statistically "deterministic" but not strictly so.
  def deterministic: Boolean = false

  def inputSchema: StructType = StructType(StructField("x", DoubleType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("tdigest", TDigestUDT) :: Nil)

  def dataType: DataType = TDigestUDT

  def initialize(buf: MutableAggregationBuffer): Unit = {
    buf(0) = TDigestSQL(TDigest.empty(deltaV, maxDiscreteV))
  }

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buf(0) = TDigestSQL(buf.getAs[TDigestSQL](0).tdigest + input.getAs[Double](0))
    }
  }

  def merge(buf1: MutableAggregationBuffer, buf2: Row): Unit = {
    buf1(0) = TDigestSQL(buf1.getAs[TDigestSQL](0).tdigest ++ buf2.getAs[TDigestSQL](0).tdigest)
  }

  def evaluate(buf: Row): Any = buf.getAs[TDigestSQL](0)
}
