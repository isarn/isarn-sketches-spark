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

/**
 * A UDAF for sketching numeric data with a TDigest.
 * Expected to be created using [[tdigestUDAF]].
 * @tparam N the expected numeric type of the data; Double, Int, etc
 * @param deltaV The delta value to be used by the TDigest object
 * @param maxDiscreteV The maxDiscrete value to be used by the TDigest object
 */
case class TDigestUDAF[N](deltaV: Double, maxDiscreteV: Int)(implicit
    num: Numeric[N],
    dataTpe: TDigestUDAFDataType[N]) extends UserDefinedAggregateFunction {

  /** customize the delta value to be used by the TDigest object */
  def delta(deltaP: Double) = this.copy(deltaV = deltaP)

  /** customize the maxDiscrete value to be used by the TDigest object */
  def maxDiscrete(maxDiscreteP: Int) = this.copy(maxDiscreteV = maxDiscreteP)

  // A t-digest is deterministic, but it is only statistically associative or commutative
  // and spark will merge partition results in nondeterministic order. That makes
  // the result of the aggregation statistically "deterministic" but not strictly so.
  def deterministic: Boolean = false

  def inputSchema: StructType = StructType(StructField("x", dataTpe.tpe) :: Nil)

  def bufferSchema: StructType = StructType(StructField("tdigest", TDigestUDT) :: Nil)

  def dataType: DataType = TDigestUDT

  def initialize(buf: MutableAggregationBuffer): Unit = {
    buf(0) = TDigestSQL(TDigest.empty(deltaV, maxDiscreteV))
  }

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buf(0) = TDigestSQL(buf.getAs[TDigestSQL](0).tdigest + num.toDouble(input.getAs[N](0)))
    }
  }

  def merge(buf1: MutableAggregationBuffer, buf2: Row): Unit = {
    buf1(0) = TDigestSQL(buf1.getAs[TDigestSQL](0).tdigest ++ buf2.getAs[TDigestSQL](0).tdigest)
  }

  def evaluate(buf: Row): Any = buf.getAs[TDigestSQL](0)
}

/** A base class that defines the common functionality for array sketching UDAFs */
abstract class TDigestMultiUDAF extends UserDefinedAggregateFunction {
  def deltaV: Double
  def maxDiscreteV: Int

  def deterministic: Boolean = false

  def bufferSchema: StructType = StructType(StructField("tdigests", TDigestArrayUDT) :: Nil)

  def dataType: DataType = TDigestArrayUDT

  def initialize(buf: MutableAggregationBuffer): Unit = {
    // we don't know vector size yet
    buf(0) = TDigestArraySQL(Array.empty[TDigest])
  }

  def merge(buf1: MutableAggregationBuffer, buf2: Row): Unit = {
    val tds2 = buf2.getAs[TDigestArraySQL](0).tdigests
    if (!tds2.isEmpty) {
      val tdt = buf1.getAs[TDigestArraySQL](0).tdigests
      val tds1 = if (!tdt.isEmpty) tdt else {
        Array.fill(tds2.length) { TDigest.empty(deltaV, maxDiscreteV) }
      }
      require(tds1.length == tds2.length)
      for { j <- 0 until tds1.length } { tds1(j) ++= tds2(j) }
      buf1(0) = TDigestArraySQL(tds1)
    }
  }

  def evaluate(buf: Row): Any = buf.getAs[TDigestArraySQL](0)
}

/**
 * A UDAF for sketching a column of ML Vectors with an array of TDigest objects.
 * Expected to be created using [[tdigestMLVecUDAF]].
 * @param deltaV The delta value to be used by the TDigest object
 * @param maxDiscreteV The maxDiscrete value to be used by the TDigest object
 */
case class TDigestMLVecUDAF(deltaV: Double, maxDiscreteV: Int) extends TDigestMultiUDAF {
  import org.apache.spark.ml.linalg.{ Vector => Vec }

  /** customize the delta value to be used by the TDigest object */
  def delta(deltaP: Double) = this.copy(deltaV = deltaP)

  /** customize the maxDiscrete value to be used by the TDigest object */
  def maxDiscrete(maxDiscreteP: Int) = this.copy(maxDiscreteV = maxDiscreteP)

  def inputSchema: StructType = StructType(StructField("vector", TDigestUDTInfra.udtVectorML) :: Nil)

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val vec = input.getAs[Vec](0)
      val tdt = buf.getAs[TDigestArraySQL](0).tdigests
      val tdigests = if (!tdt.isEmpty) tdt else {
        Array.fill(vec.size) { TDigest.empty(deltaV, maxDiscreteV) }
      }
      require(tdigests.length == vec.size)
      vec match {
        case v: org.apache.spark.ml.linalg.SparseVector =>
          var jBeg = 0
          v.foreachActive((j, x) => {
            for { k <- jBeg until j } { tdigests(k) += 0.0 }
            tdigests(j) += x
            jBeg = j + 1
          })
          for { k <- jBeg until vec.size } { tdigests(k) += 0.0 }
        case _ =>
          for { j <- 0 until vec.size } { tdigests(j) += vec(j) }
      }
      buf(0) = TDigestArraySQL(tdigests)
    }
  }
}

/**
 * A UDAF for sketching a column of MLLib Vectors with an array of TDigest objects.
 * Expected to be created using [[tdigestMLLibVecUDAF]].
 * @param deltaV The delta value to be used by the TDigest object
 * @param maxDiscreteV The maxDiscrete value to be used by the TDigest object
 */
case class TDigestMLLibVecUDAF(deltaV: Double, maxDiscreteV: Int) extends TDigestMultiUDAF {
  import org.apache.spark.mllib.linalg.{ Vector => Vec }

  /** customize the delta value to be used by the TDigest object */
  def delta(deltaP: Double) = this.copy(deltaV = deltaP)

  /** customize the maxDiscrete value to be used by the TDigest object */
  def maxDiscrete(maxDiscreteP: Int) = this.copy(maxDiscreteV = maxDiscreteP)

  def inputSchema: StructType =
    StructType(StructField("vector", TDigestUDTInfra.udtVectorMLLib) :: Nil)

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val vec = input.getAs[Vec](0)
      val tdt = buf.getAs[TDigestArraySQL](0).tdigests
      val tdigests = if (!tdt.isEmpty) tdt else {
        Array.fill(vec.size) { TDigest.empty(deltaV, maxDiscreteV) }
      }
      require(tdigests.length == vec.size)
      vec match {
        case v: org.apache.spark.mllib.linalg.SparseVector =>
          var jBeg = 0
          v.foreachActive((j, x) => {
            for { k <- jBeg until j } { tdigests(k) += 0.0 }
            tdigests(j) += x
            jBeg = j + 1
          })
          for { k <- jBeg until vec.size } { tdigests(k) += 0.0 }
        case _ =>
          for { j <- 0 until vec.size } { tdigests(j) += vec(j) }
      }
      buf(0) = TDigestArraySQL(tdigests)
    }
  }
}

/**
 * A UDAF for sketching a column of numeric ArrayData with an array of TDigest objects.
 * Expected to be created using [[tdigestArrayUDAF]].
 * @tparam N the expected numeric type of the data; Double, Int, etc
 * @param deltaV The delta value to be used by the TDigest objects
 * @param maxDiscreteV The maxDiscrete value to be used by the TDigest objects
 */
case class TDigestArrayUDAF[N](deltaV: Double, maxDiscreteV: Int)(implicit
    num: Numeric[N],
    dataTpe: TDigestUDAFDataType[N]) extends TDigestMultiUDAF {

  /** customize the delta value to be used by the TDigest object */
  def delta(deltaP: Double) = this.copy(deltaV = deltaP)

  /** customize the maxDiscrete value to be used by the TDigest object */
  def maxDiscrete(maxDiscreteP: Int) = this.copy(maxDiscreteV = maxDiscreteP)

  def inputSchema: StructType =
    StructType(StructField("array", ArrayType(dataTpe.tpe, true)) :: Nil)

  def update(buf: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val data = input.getSeq[N](0)
      val tdt = buf.getAs[TDigestArraySQL](0).tdigests
      val tdigests = if (!tdt.isEmpty) tdt else {
        Array.fill(data.length) { TDigest.empty(deltaV, maxDiscreteV) }
      }
      require(tdigests.length == data.length)
      var j = 0
      for { x <- data } {
        if (x != null) tdigests(j) += num.toDouble(x)
        j += 1
      }
      buf(0) = TDigestArraySQL(tdigests)
    }
  }
}
