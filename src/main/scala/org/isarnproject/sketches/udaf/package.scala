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

package org.isarnproject.sketches

import scala.language.implicitConversions

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util._

import org.apache.spark.isarnproject.sketches.udt._

/** package-wide methods, implicits and definitions for sketching UDAFs */
package object udaf {
  /**
   * Obtain a UDAF for sketching a single numeric Dataset column using a t-digest
   * @tparam N The numeric type of the column; Double, Int, etc
   * @return A UDAF that can be applied to a Dataset column
   * @example
   * {{{
   * import org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._
   * // create a UDAF for a t-digest, adding custom settings for delta and maxDiscrete
   * val udafTD = tdigestUDAF[Double].delta(0.1).maxDiscrete(25)
   * // apply the UDAF to get a t-digest for a data column
   * val agg = data.agg(udafTD($"NumericColumn"))
   * // extract the t-digest
   * val td = agg.getAs[TDigestSQL](0).tdigest
   * }}}
   */
  def tdigestUDAF[N](implicit
    num: Numeric[N],
    dataType: TDigestUDAFDataType[N])
  = TDigestUDAF(TDigest.deltaDefault, 0)

  /**
   * Obtain a UDAF for sketching a numeric array-data Dataset column, using a t-digest for
   * each element.
   * @tparam N The numeric type of the array-data column; Double, Int, etc
   * @return A UDAF that can be applied to a Dataset array-data column
   * @example
   * {{{
   * import org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._
   * // create a UDAF for t-digest array, adding custom settings for delta and maxDiscrete
   * val udafTD = tdigestArrayUDAF[Double].delta(0.1).maxDiscrete(25)
   * // apply the UDAF to get an array of t-digests for each element in the array-data
   * val agg = data.agg(udafTD($"NumericArrayColumn"))
   * // extract the t-digest array
   * val tdArray = agg.getAs[TDigestArraySQL](0).tdigests
   * }}}
   */
  def tdigestArrayUDAF[N](implicit
    num: Numeric[N],
    dataType: TDigestUDAFDataType[N])
  = TDigestArrayUDAF(TDigest.deltaDefault, 0)

  /**
   * Obtain a UDAF for sketching an ML Vector Dataset column, using a t-digest for
   * each element in the vector
   * @return A UDAF that can be applied to a ML Vector column
   * @example
   * {{{
   * import org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._
   * // create a UDAF for t-digest array, adding custom settings for delta and maxDiscrete
   * val udafTD = tdigestMLVecUDAF[Double].delta(0.1).maxDiscrete(25)
   * // apply the UDAF to get an array of t-digests for each element in the array-data
   * val agg = data.agg(udafTD($"MLVecColumn"))
   * // extract the t-digest array
   * val tdArray = agg.getAs[TDigestArraySQL](0).tdigests
   * }}}
   */
  def tdigestMLVecUDAF = TDigestMLVecUDAF(TDigest.deltaDefault, 0)

  /**
   * Obtain a UDAF for sketching an MLLib Vector Dataset column, using a t-digest for
   * each element in the vector
   * @return A UDAF that can be applied to a MLLib Vector column
   * @example
   * {{{
   * import org.isarnproject.sketches.udaf._, org.apache.spark.isarnproject.sketches.udt._
   * // create a UDAF for t-digest array, adding custom settings for delta and maxDiscrete
   * val udafTD = tdigestMLLibVecUDAF[Double].delta(0.1).maxDiscrete(25)
   * // apply the UDAF to get an array of t-digests for each element in the array-data
   * val agg = data.agg(udafTD($"MLLibVecColumn"))
   * // extract the t-digest array
   * val tdArray = agg.getAs[TDigestArraySQL](0).tdigests
   * }}}
   */
  def tdigestMLLibVecUDAF = TDigestMLLibVecUDAF(TDigest.deltaDefault, 0)

  /**
   * Obtain a UDAF for aggregating (reducing) a column of t-digests
   * @return A UDAF that can be applied to a column of t-digests
   */
  def tdigestReduceUDAF = TDigestReduceUDAF(TDigest.deltaDefault, 0)

  /**
   * Obtain a UDAF for aggregating (reducing) a column of t-digest vectors
   * @return A UDAF that can be applied to a column of t-digest vectors
   */
  def tdigestArrayReduceUDAF = TDigestArrayReduceUDAF(TDigest.deltaDefault, 0)

  /** implicitly unpack a TDigestSQL to extract its TDigest payload */
  implicit def implicitTDigestSQLToTDigest(tdsql: TDigestSQL): TDigest = tdsql.tdigest

  /** implicitly unpack a TDigestArraySQL to extract its Array[TDigest] payload */
  implicit def implicitTDigestArraySQLToTDigestArray(tdasql: TDigestArraySQL): Array[TDigest] =
    tdasql.tdigests

  /** For declaring implicit values that map numeric types to corresponding DataType values */
  case class TDigestUDAFDataType[N](tpe: DataType)

  implicit val tDigestUDAFDataTypeByte = TDigestUDAFDataType[Byte](ByteType)
  implicit val tDigestUDAFDataTypeShort = TDigestUDAFDataType[Short](ShortType)
  implicit val tDigestUDAFDataTypeInt = TDigestUDAFDataType[Int](IntegerType)
  implicit val tDigestUDAFDataTypeLong = TDigestUDAFDataType[Long](LongType)
  implicit val tDigestUDAFDataTypeFloat = TDigestUDAFDataType[Float](FloatType)
  implicit val tDigestUDAFDataTypeDouble = TDigestUDAFDataType[Double](DoubleType)
}
