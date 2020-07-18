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

/**
 * Aggregators and supporting definitions for using T-Digest sketches with Spark
 * DataFrame and Dataset.
 */
package org.isarnproject.sketches.spark.tdigest {

import org.apache.spark.sql.types.SQLUserDefinedType
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{ Encoder, Encoders }
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

// Every time I do this I die a little inside
import org.apache.spark.mllib.linalg.{ Vector => MLLibVec }
import org.apache.spark.ml.linalg.{ Vector => MLVec }

import org.isarnproject.sketches.spark.infra.ScalarNumeric

import infra.TDigest

/**
 * A spark Aggregator for sketching a numeric column with a t-digest
 * @tparam V the numeric value type
 * @param compression the t-digest compression parameter.
 * @param maxDiscrete maximum number of discrete values to track in PMF mode.
 */
class TDigestAggregator[V](compression: Double, maxDiscrete: Int)(
  implicit
    vnum: ScalarNumeric[V])
  extends
    Aggregator[V, TDigest, TDigest]
{
  def zero: TDigest = new TDigest(compression, maxDiscrete)
  def reduce(td: TDigest, v: V): TDigest = {
    if (v != null) td.update(vnum.toDouble(v))
    td
  }
  def merge(td1: TDigest, td2: TDigest): TDigest = {
    td1.merge(td2)
    td1
  }
  def finish(td: TDigest): TDigest = td
  def bufferEncoder: Encoder[TDigest] = ExpressionEncoder[TDigest]()
  def outputEncoder: Encoder[TDigest] = ExpressionEncoder[TDigest]()
}

/** Companion object and definitions for TDigestAggregator */
object TDigestAggregator {
  import scala.reflect.runtime.universe.TypeTag
  import org.apache.spark.sql.functions.udaf
  import org.apache.spark.sql.expressions.UserDefinedFunction

  /**
   * Construct a TDigestAggregator
   * @tparam V the numeric value type expected from input column
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def apply[V](
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault)(
    implicit
      vnum: ScalarNumeric[V]): TDigestAggregator[V] =
    new TDigestAggregator[V](compression, maxDiscrete)

  /**
   * Return a spark UDF, usable with DataFrames,
   * that aggregates a numeric column using a t-digest.
   * @tparam V the numeric value type expected from input column
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def udf[V](
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault)(
    implicit
      vnum: ScalarNumeric[V],
      ttV: TypeTag[V]): UserDefinedFunction =
    udaf(apply[V](compression, maxDiscrete))
}

/**
 * A base class for aggregating over array-like collections of values.
 * Supplies all Aggregator methods except `reduce`, which is class specific.
 * Maintains and returns an Array[TDigest], regardless of specific input type.
 * @tparam V the specific array-like column type expected from input column
 */
abstract class TDigestArrayAggregatorBase[V]
  extends
    Aggregator[V, Array[TDigest], Array[TDigest]] {
  def zero: Array[TDigest] = Array.empty[TDigest]
  def merge(td1: Array[TDigest], td2: Array[TDigest]): Array[TDigest] = {
    if      (td1.isEmpty) td2
    else if (td2.isEmpty) td1
    else {
      require(td1.length == td2.length)
      for { j <- 0 until td1.length } { td1(j).merge(td2(j)) }
      td1
    }
  }
  def finish(td: Array[TDigest]): Array[TDigest] = td
  def bufferEncoder: Encoder[Array[TDigest]] = ExpressionEncoder[Array[TDigest]]()
  def outputEncoder: Encoder[Array[TDigest]] = ExpressionEncoder[Array[TDigest]]()
}

/**
 * A spark Aggregator for sketching an array of numeric values with an array of t-digests
 * @tparam V the numeric value type contained in the array
 * @param compression the t-digest compression parameter.
 * @param maxDiscrete maximum number of discrete values to track in PMF mode.
 */
class TDigestArrayAggregator[V](
    compression: Double,
    maxDiscrete: Int)(
  implicit
    vnum: ScalarNumeric[V])
  extends
    TDigestArrayAggregatorBase[Array[V]] {

  def reduce(tdai: Array[TDigest], data: Array[V]): Array[TDigest] = {
    if (data == null) tdai else {
      val tda = if (!tdai.isEmpty || data.isEmpty) tdai else
        Array.fill(data.length) { new TDigest(compression, maxDiscrete) }
      require(tda.length == data.length)
      for { j <- 0 until tda.length } { tda(j).update(vnum.toDouble(data(j))) }
      tda
    }
  }
}

/** Companion object and definitions */
object TDigestArrayAggregator {
  import scala.reflect.runtime.universe.TypeTag
  import org.apache.spark.sql.functions.udaf
  import org.apache.spark.sql.expressions.UserDefinedFunction

  /**
   * Construct a TDigestArrayAggregator
   * @tparam V the numeric value type contained in the array column
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def apply[V](
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault)(
    implicit
      vnum: ScalarNumeric[V]): TDigestArrayAggregator[V] =
    new TDigestArrayAggregator[V](compression, maxDiscrete)

  /**
   * Return a spark UDF, usable with DataFrames,
   * that aggregates a numeric array column using an array of t-digests.
   * @tparam V the numeric value type expected from input column
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def udf[V](
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault)(
    implicit
      vnum: ScalarNumeric[V],
      ttV: TypeTag[V]): UserDefinedFunction =
    udaf(apply[V](compression, maxDiscrete))
}

/**
 * A spark Aggregator for sketching the values in an MLLib Vector with an array of t-digests
 * @param compression the t-digest compression parameter.
 * @param maxDiscrete maximum number of discrete values to track in PMF mode.
 */
class TDigestMLLibVecAggregator(
    compression: Double,
    maxDiscrete: Int)
  extends
    TDigestArrayAggregatorBase[MLLibVec] {

  def reduce(tdai: Array[TDigest], data: MLLibVec): Array[TDigest] = {
    if (data == null) tdai else {
      val tda = if (!tdai.isEmpty || (data.size == 0)) tdai else
        Array.fill(data.size) { new TDigest(compression, maxDiscrete) }
      require(tda.length == data.size)
      data match {
        case v: org.apache.spark.mllib.linalg.SparseVector =>
          var jBeg = 0
          v.foreachActive((j, x) => {
            for { k <- jBeg until j } { tda(k).update(0.0) }
            tda(j).update(x)
            jBeg = j + 1
          })
          for { k <- jBeg until data.size } { tda(k).update(0.0) }
        case _ =>
          for { j <- 0 until data.size } { tda(j).update(data(j)) }
      }
      tda
    }
  }
}

/** Companion object and definitions */
object TDigestMLLibVecAggregator {
  import scala.reflect.runtime.universe.TypeTag
  import org.apache.spark.sql.functions.udaf
  import org.apache.spark.sql.expressions.UserDefinedFunction

  /**
   * Construct a TDigestMLLibVecAggregator
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def apply(
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault): TDigestMLLibVecAggregator =
    new TDigestMLLibVecAggregator(compression, maxDiscrete)

  /**
   * Return a spark UDF, usable with DataFrames,
   * that aggregates an MLLib Vector column using an array of t-digests.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def udf(
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault): UserDefinedFunction =
    udaf(apply(compression, maxDiscrete))
}

/**
 * A spark Aggregator for sketching the values in an ML Vector with an array of t-digests
 * @param compression the t-digest compression parameter.
 * @param maxDiscrete maximum number of discrete values to track in PMF mode.
 */
class TDigestMLVecAggregator(
    compression: Double,
    maxDiscrete: Int)
  extends
    TDigestArrayAggregatorBase[MLVec] {

  def reduce(tdai: Array[TDigest], data: MLVec): Array[TDigest] = {
    if (data == null) tdai else {
      val tda = if (!tdai.isEmpty || (data.size == 0)) tdai else
        Array.fill(data.size) { new TDigest(compression, maxDiscrete) }
      require(tda.length == data.size)
      data match {
        case v: org.apache.spark.ml.linalg.SparseVector =>
          var jBeg = 0
          v.foreachActive((j, x) => {
            for { k <- jBeg until j } { tda(k).update(0.0) }
            tda(j).update(x)
            jBeg = j + 1
          })
          for { k <- jBeg until data.size } { tda(k).update(0.0) }
        case _ =>
          for { j <- 0 until data.size } { tda(j).update(data(j)) }
      }
      tda
    }
  }
}

/** Companion object and definitions */
object TDigestMLVecAggregator {
  import scala.reflect.runtime.universe.TypeTag
  import org.apache.spark.sql.functions.udaf
  import org.apache.spark.sql.expressions.UserDefinedFunction

  /**
   * Construct a TDigestMLVecAggregator
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def apply(
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault): TDigestMLVecAggregator =
    new TDigestMLVecAggregator(compression, maxDiscrete)

  /**
   * Return a spark UDF, usable with DataFrames,
   * that aggregates an ML Vector column using an array of t-digests.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def udf(
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault): UserDefinedFunction =
    udaf(apply(compression, maxDiscrete))
}

/**
 * A spark Aggregator for reducing a column of t-digests down to a single t-digest.
 * Therefore, in this Aggregator, 'reduce' and 'merge' both perform a t-digest merge operation.
 * @param compression the t-digest compression parameter.
 * @param maxDiscrete maximum number of discrete values to track in PMF mode.
 */
class TDigestReduceAggregator(
    compression: Double,
    maxDiscrete: Int)
  extends
    Aggregator[TDigest, TDigest, TDigest]
{
  def zero: TDigest = new TDigest(compression, maxDiscrete)
  def reduce(td: TDigest, tdi: TDigest): TDigest = {
    if (tdi != null) td.merge(tdi)
    td
  }
  def merge(td1: TDigest, td2: TDigest): TDigest = {
    td1.merge(td2)
    td1
  }
  def finish(td: TDigest): TDigest = td
  def bufferEncoder: Encoder[TDigest] = ExpressionEncoder[TDigest]()
  def outputEncoder: Encoder[TDigest] = ExpressionEncoder[TDigest]()
}

/** Companion object and definitions */
object TDigestReduceAggregator {
  import scala.reflect.runtime.universe.TypeTag
  import org.apache.spark.sql.functions.udaf
  import org.apache.spark.sql.expressions.UserDefinedFunction

  /**
   * Construct a TDigestReduceAggregator
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def apply(
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault): TDigestReduceAggregator =
    new TDigestReduceAggregator(compression, maxDiscrete)

  /**
   * Return a spark UDF, usable with DataFrames,
   * that aggregates a column of t-digests down to a single t-digest.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def udf(
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault): UserDefinedFunction =
    udaf(apply(compression, maxDiscrete))
}

/**
 * A spark Aggregator for reducing a column containing arrays of t-digests
 * down to a single array of t-digests.
 * Therefore, in this Aggregator, 'reduce' and 'merge' both perform t-digest merge operations.
 * @param compression the t-digest compression parameter.
 * @param maxDiscrete maximum number of discrete values to track in PMF mode.
 */
class TDigestArrayReduceAggregator(
    compression: Double,
    maxDiscrete: Int)
  extends
    TDigestArrayAggregatorBase[Array[TDigest]] {

  def reduce(tdai: Array[TDigest], data: Array[TDigest]): Array[TDigest] = {
    if (data == null) tdai else {
      val tda = if (!tdai.isEmpty || data.isEmpty) tdai else
        Array.fill(data.length) { new TDigest(compression, maxDiscrete) }
      require(tda.length == data.length)
      for { j <- 0 until tda.length } { tda(j).merge(data(j)) }
      tda
    }
  }
}

/** Companion object and definitions */
object TDigestArrayReduceAggregator {
  import scala.reflect.runtime.universe.TypeTag
  import org.apache.spark.sql.functions.udaf
  import org.apache.spark.sql.expressions.UserDefinedFunction

  /**
   * Construct a TDigestArrayReduceAggregator
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def apply(
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault): TDigestArrayReduceAggregator =
    new TDigestArrayReduceAggregator(compression, maxDiscrete)

  /**
   * Return a spark UDF, usable with DataFrames,
   * that aggregates a column of t-digest arrays down to a single t-digest array.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   */
  def udf(
      compression: Double = TDigest.compressionDefault,
      maxDiscrete: Int = TDigest.maxDiscreteDefault): UserDefinedFunction =
    udaf(apply(compression, maxDiscrete))
}

/**
 * Convenience functions that do not require type parameters or typeclasses to invoke.
 * Use cases include calling from java or supporting pyspark bindings.
 */
object functions {
  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of integer values with a t-digest.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestIntUDF(compression: Double, maxDiscrete: Int) =
    TDigestAggregator.udf[Int](compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of long-integer values with a t-digest.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestLongUDF(compression: Double, maxDiscrete: Int) =
    TDigestAggregator.udf[Long](compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of float values with a t-digest.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestFloatUDF(compression: Double, maxDiscrete: Int) =
    TDigestAggregator.udf[Float](compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of double values with a t-digest.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestDoubleUDF(compression: Double, maxDiscrete: Int) =
    TDigestAggregator.udf[Double](compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of integer arrays with an array of t-digests.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestIntArrayUDF(compression: Double, maxDiscrete: Int) =
    TDigestArrayAggregator.udf[Int](compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of long-integer arrays with an array of t-digests.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestLongArrayUDF(compression: Double, maxDiscrete: Int) =
    TDigestArrayAggregator.udf[Long](compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of float arrays with an array of t-digests.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestFloatArrayUDF(compression: Double, maxDiscrete: Int) =
    TDigestArrayAggregator.udf[Float](compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of double arrays with an array of t-digests.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestDoubleArrayUDF(compression: Double, maxDiscrete: Int) =
    TDigestArrayAggregator.udf[Double](compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of MLLib Vectors with an array of t-digests.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestMLLibVecUDF(compression: Double, maxDiscrete: Int) =
    TDigestMLLibVecAggregator.udf(compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of ML Vectors with an array of t-digests.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestMLVecUDF(compression: Double, maxDiscrete: Int) =
    TDigestMLVecAggregator.udf(compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of t-digests into a single t-digest.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestReduceUDF(compression: Double, maxDiscrete: Int) =
    TDigestReduceAggregator.udf(compression, maxDiscrete)

  /**
   * Obtain a UDF, usable with DataFrames,
   * for aggregating a column of t-digest arrays into a single t-digest array.
   * @param compression the t-digest compression parameter.
   * @param maxDiscrete maximum number of discrete values to track in PMF mode.
   * @return the new aggregating UDF
   */
  def tdigestArrayReduceUDF(compression: Double, maxDiscrete: Int) =
    TDigestArrayReduceAggregator.udf(compression, maxDiscrete)
}

package infra {
  import org.isarnproject.sketches.java.{ TDigest => BaseTD }
  import org.apache.spark.isarnproject.sketches.tdigest.udt.TDigestUDT

  // the only reason for this shim class is to link it to TDigestUDT
  // the user does not need to see this shim, and can do:
  // resultRow.getAs[org.isarnproject.sketches.java.TDigest](0)
  @SQLUserDefinedType(udt = classOf[TDigestUDT])
  class TDigest(
      compression: Double,
      maxDiscrete: Int,
      cent: Array[Double],
      mass: Array[Double])
    extends
      BaseTD(compression, maxDiscrete, cent, mass) {

    def this(
        compression: Double = TDigest.compressionDefault,
        maxDiscrete: Int = TDigest.maxDiscreteDefault) {
      this(compression, maxDiscrete, null, null)
    }
  }

  object TDigest {
    val compressionDefault: Double = 0.5
    val maxDiscreteDefault: Int = 0
  }
}

} // package org.isarnproject.sketches.spark.tdigest

// I need to accept that Spark is never going to fix this.
package org.apache.spark.isarnproject.sketches.tdigest.udt {

import java.util.Arrays

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}

import org.isarnproject.sketches.spark.tdigest.infra.TDigest

class TDigestUDT extends UserDefinedType[TDigest] {
  def userClass: Class[TDigest] = classOf[TDigest]

  override def pyUDT: String = "isarnproject.sketches.spark.tdigest.TDigestUDT"

  override def typeName: String = "tdigest"

  override def equals(obj: Any): Boolean = {
    obj match {
      case _: TDigestUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = classOf[TDigestUDT].getName.hashCode()

  private[spark] override def asNullable: TDigestUDT = this

  def sqlType: DataType = StructType(
    StructField("compression", DoubleType, false) ::
    StructField("maxDiscrete", IntegerType, false) ::
    StructField("cent", ArrayType(DoubleType, false), false) ::
    StructField("mass", ArrayType(DoubleType, false), false) ::
    Nil)

  def serialize(td: TDigest): Any = {
    val row = new GenericInternalRow(4)
    row.setDouble(0, td.getCompression())
    row.setInt(1, td.getMaxDiscrete())
    val clustX = java.util.Arrays.copyOf(td.getCentUnsafe(), td.size())
    row.update(2, UnsafeArrayData.fromPrimitiveArray(clustX))
    val clustM = java.util.Arrays.copyOf(td.getMassUnsafe(), td.size())
    row.update(3, UnsafeArrayData.fromPrimitiveArray(clustM))
    row
  }

  def deserialize(datum: Any): TDigest = datum match {
    case row: InternalRow =>
      require(row.numFields == 4, s"expected row length 4, got ${row.numFields}")
      val compression = row.getDouble(0)
      val maxDiscrete = row.getInt(1)
      val clustX = row.getArray(2).toDoubleArray()
      val clustM = row.getArray(3).toDoubleArray()
      new TDigest(compression, maxDiscrete, clustX, clustM)
    case u => throw new Exception(s"failed to deserialize: $u")
  }
}

/** Shims for exposing Spark's VectorUDT objects outside of org.apache.spark scope */
object infra {
  private object udtML extends org.apache.spark.ml.linalg.VectorUDT
  def udtVectorML: DataType = udtML

  private object udtMLLib extends org.apache.spark.mllib.linalg.VectorUDT
  def udtVectorMLLib: DataType = udtMLLib
}

} // package
