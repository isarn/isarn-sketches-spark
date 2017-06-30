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

package org.apache.spark.isarnproject.sketches.udt

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.isarnproject.sketches.TDigest
import org.isarnproject.sketches.tdmap.TDigestMap

@SQLUserDefinedType(udt = classOf[TDigestUDT])
case class TDigestSQL(tdigest: TDigest)

class TDigestUDT extends UserDefinedType[TDigestSQL] {
  def userClass: Class[TDigestSQL] = classOf[TDigestSQL]

  def sqlType: DataType = StructType(
    StructField("delta", DoubleType, false) ::
    StructField("maxDiscrete", IntegerType, false) ::
    StructField("nclusters", IntegerType, false) ::
    StructField("clustX", ArrayType(DoubleType, false), false) ::
    StructField("clustM", ArrayType(DoubleType, false), false) ::
    Nil)

  def serialize(tdsql: TDigestSQL): Any = {
    val TDigestSQL(TDigest(delta, maxDiscrete, nclusters, clusters)) = tdsql
    val row = new GenericInternalRow(5)
    row.setDouble(0, delta)
    row.setInt(1, maxDiscrete)
    row.setInt(2, nclusters)
    val clustX = clusters.keys.toArray
    val clustM = clusters.values.toArray
    row.update(3, UnsafeArrayData.fromPrimitiveArray(clustX))
    row.update(4, UnsafeArrayData.fromPrimitiveArray(clustM))
    row
  }

  def deserialize(datum: Any): TDigestSQL = datum match {
    case row: InternalRow =>
      require(row.numFields == 5, s"expected row length 5, got ${row.numFields}")
      val delta = row.getDouble(0)
      val maxDiscrete = row.getInt(1)
      val nclusters = row.getInt(2)
      val clustX = row.getArray(3).toDoubleArray()
      val clustM = row.getArray(4).toDoubleArray()
      val clusters = clustX.zip(clustM)
        .foldLeft(TDigestMap.empty) { case (td, e) => td + e }
      TDigestSQL(TDigest(delta, maxDiscrete, nclusters, clusters))
    case u => throw new Exception(s"failed to deserialize: $u")
  }
}
