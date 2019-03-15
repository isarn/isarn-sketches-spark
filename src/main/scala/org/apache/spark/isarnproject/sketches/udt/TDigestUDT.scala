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
import org.isarnproject.sketches.java.TDigest

/** A type for receiving the results of deserializing [[TDigestUDT]].
 * The payload is the tdigest member field, holding a TDigest object.
 * This is necessary because (a) I define the TDigest type is defined in the isarn-sketches
 * package and I do not
 * want any Spark dependencies on that package, and (b) Spark has privatized UserDefinedType under
 * org.apache.spark scope, and so I have to have a paired result type in the same scope.
 * @param tdigest The TDigest payload, which does the actual sketching.
 */
@SQLUserDefinedType(udt = classOf[TDigestUDT])
case class TDigestSQL(tdigest: TDigest)

/** A UserDefinedType for serializing and deserializing [[TDigestSQL]] structures during UDAF
 * aggregations.
 */
class TDigestUDT extends UserDefinedType[TDigestSQL] {
  def userClass: Class[TDigestSQL] = classOf[TDigestSQL]

  override def pyUDT: String = "isarnproject.sketches.udt.tdigest.TDigestUDT"

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
    StructField("delta", DoubleType, false) ::
    StructField("maxDiscrete", IntegerType, false) ::
    StructField("clustX", ArrayType(DoubleType, false), false) ::
    StructField("clustM", ArrayType(DoubleType, false), false) ::
    Nil)

  def serialize(tdsql: TDigestSQL): Any = serializeTD(tdsql.tdigest)

  def deserialize(datum: Any): TDigestSQL = TDigestSQL(deserializeTD(datum))

  private[sketches] def serializeTD(td: TDigest): InternalRow = {
    val row = new GenericInternalRow(4)
    row.setDouble(0, td.getCompression())
    row.setInt(1, td.getMaxDiscrete())
    val clustX = td.getCentUnsafe()
    val clustM = td.getMassUnsafe()
    row.update(2, UnsafeArrayData.fromPrimitiveArray(clustX))
    row.update(3, UnsafeArrayData.fromPrimitiveArray(clustM))
    row
  }

  private[sketches] def deserializeTD(datum: Any): TDigest = datum match {
    case row: InternalRow if (row.numFields == 4) =>
      val delta = row.getDouble(0)
      val maxDiscrete = row.getInt(1)
      val clustX = row.getArray(2).toDoubleArray()
      val clustM = row.getArray(3).toDoubleArray()
      new TDigest(delta, maxDiscrete, clustX, clustM)
    case u => throw new Exception(s"failed to deserialize: $u")
  }
}

/** Instantiated instance of [[TDigestUDT]] for use by UDAF objects */
case object TDigestUDT extends TDigestUDT

/** A type for receiving the results of deserializing [[TDigestArrayUDT]].
 * The payload is the tdigests member field, holding an Array of TDigest objects.
 * @param tdigests An array of TDigest objects, which do the actual sketching.
 * @see [[TDigestSQL]] for additional context
 */
@SQLUserDefinedType(udt = classOf[TDigestArrayUDT])
case class TDigestArraySQL(tdigests: Array[TDigest])

/**
 * A UserDefinedType for serializing and deserializing [[TDigestArraySQL]] objects
 * during UDAF aggregations
 */
class TDigestArrayUDT extends UserDefinedType[TDigestArraySQL] {
  def userClass: Class[TDigestArraySQL] = classOf[TDigestArraySQL]

  override def pyUDT: String = "isarnproject.sketches.udt.tdigest.TDigestArrayUDT"

  override def typeName: String = "tdigestarray"

  override def equals(obj: Any): Boolean = {
    obj match {
      case _: TDigestArrayUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = classOf[TDigestArrayUDT].getName.hashCode()

  private[spark] override def asNullable: TDigestArrayUDT = this

  // Spark seems to have trouble with ArrayType data that isn't
  // serialized using UnsafeArrayData (SPARK-21277), so my workaround
  // is to store all the cluster information flattened into single Unsafe arrays.
  // To deserialize, I unpack the slices.
  def sqlType: DataType = StructType(
    StructField("delta", DoubleType, false) ::
    StructField("maxDiscrete", IntegerType, false) ::
    StructField("clusterS", ArrayType(IntegerType, false), false) ::
    StructField("clusterX", ArrayType(DoubleType, false), false) ::
    StructField("ClusterM", ArrayType(DoubleType, false), false) ::
    Nil)

  def serialize(tdasql: TDigestArraySQL): Any = {
    val row = new GenericInternalRow(5)
    val tda: Array[TDigest] = tdasql.tdigests
    val delta = if (tda.isEmpty) 0.0 else tda.head.getCompression()
    val maxDiscrete = if (tda.isEmpty) 0 else tda.head.getMaxDiscrete()
    val clustS = tda.map(_.size())
    val clustX = tda.flatMap(_.getCentUnsafe())
    val clustM = tda.flatMap(_.getMassUnsafe())
    row.setDouble(0, delta)
    row.setInt(1, maxDiscrete)
    row.update(2, UnsafeArrayData.fromPrimitiveArray(clustS))
    row.update(3, UnsafeArrayData.fromPrimitiveArray(clustX))
    row.update(4, UnsafeArrayData.fromPrimitiveArray(clustM))
    row
  }

  def deserialize(datum: Any): TDigestArraySQL = datum match {
    case row: InternalRow =>
      require(row.numFields == 5, s"expected row length 5, got ${row.numFields}")
      val delta = row.getDouble(0)
      val maxDiscrete = row.getInt(1)
      val clustS = row.getArray(2).toIntArray()
      val clustX = row.getArray(3).toDoubleArray()
      val clustM = row.getArray(4).toDoubleArray()
      var beg = 0
      val tda = clustS.map { nclusters =>
        val x = clustX.slice(beg, beg + nclusters)
        val m = clustM.slice(beg, beg + nclusters)
        val td = new TDigest(delta, maxDiscrete, x, m)
        beg += nclusters
        td
      }
      TDigestArraySQL(tda)
    case u => throw new Exception(s"failed to deserialize: $u")
  }
}

/** A [[TDigestArrayUDT]] instance for use in declaring UDAF objects */
case object TDigestArrayUDT extends TDigestArrayUDT

/** Shims for exposing Spark's VectorUDT objects outside of org.apache.spark scope */
object TDigestUDTInfra {
  private val udtML = new org.apache.spark.ml.linalg.VectorUDT
  def udtVectorML: DataType = udtML

  private val udtMLLib = new org.apache.spark.mllib.linalg.VectorUDT
  def udtVectorMLLib: DataType = udtMLLib
}
