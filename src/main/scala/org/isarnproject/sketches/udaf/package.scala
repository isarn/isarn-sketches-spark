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

package object udaf {
  def tdigestUDAF[N](implicit
    num: Numeric[N],
    dataType: TDigestUDAFDataType[N])
  = TDigestUDAF(TDigest.deltaDefault, 0)

  def tdigestArrayUDAF[N](implicit
    num: Numeric[N],
    dataType: TDigestUDAFDataType[N])
  = TDigestArrayUDAF(TDigest.deltaDefault, 0)

  def tdigestMLVecUDAF = TDigestMLVecUDAF(TDigest.deltaDefault, 0)

  def tdigestMLLibVecUDAF = TDigestMLLibVecUDAF(TDigest.deltaDefault, 0)

  implicit def implicitTDigestSQLToTDigest(tdsql: TDigestSQL): TDigest = tdsql.tdigest
  implicit def implicitTDigestArraySQLToTDigestArray(tdasql: TDigestArraySQL): Array[TDigest] =
    tdasql.tdigests

  case class TDigestUDAFDataType[N](tpe: DataType)

  implicit val tDigestUDAFDataTypeByte = TDigestUDAFDataType[Byte](ByteType)
  implicit val tDigestUDAFDataTypeShort = TDigestUDAFDataType[Short](ShortType)
  implicit val tDigestUDAFDataTypeInt = TDigestUDAFDataType[Int](IntegerType)
  implicit val tDigestUDAFDataTypeLong = TDigestUDAFDataType[Long](LongType)
  implicit val tDigestUDAFDataTypeFloat = TDigestUDAFDataType[Float](FloatType)
  implicit val tDigestUDAFDataTypeDouble = TDigestUDAFDataType[Double](DoubleType)
}
