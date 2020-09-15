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

package org.isarnproject.sketches.spark.infra

// scala's standard Numeric doesn't support java.lang.xxx
trait ScalarNumeric[N] extends Serializable {
  def toDouble(v: N): Double
}
object ScalarNumeric {
  implicit val javaIntIsSN: ScalarNumeric[java.lang.Integer] =
    new ScalarNumeric[java.lang.Integer] {
      @inline def toDouble(v: java.lang.Integer): Double = v.toDouble
    }
  implicit val javaLongIsSN: ScalarNumeric[java.lang.Long] =
    new ScalarNumeric[java.lang.Long] {
      @inline def toDouble(v: java.lang.Long): Double = v.toDouble
    }
  implicit val javaFloatIsSN: ScalarNumeric[java.lang.Float] =
    new ScalarNumeric[java.lang.Float] {
      @inline def toDouble(v: java.lang.Float): Double = v.toDouble
    }
  implicit val javaDoubleIsSN: ScalarNumeric[java.lang.Double] =
    new ScalarNumeric[java.lang.Double] {
      @inline def toDouble(v: java.lang.Double): Double = v
    }
  implicit val scalaIntIsSN: ScalarNumeric[Int] =
    new ScalarNumeric[Int] {
      @inline def toDouble(v: Int): Double = v.toDouble
    }
  implicit val scalaLongIsSN: ScalarNumeric[Long] =
    new ScalarNumeric[Long] {
      @inline def toDouble(v: Long): Double = v.toDouble
    }
  implicit val scalaFloatIsSN: ScalarNumeric[Float] =
    new ScalarNumeric[Float] {
      @inline def toDouble(v: Float): Double = v.toDouble
    }
  implicit val scalaDoubleIsSN: ScalarNumeric[Double] =
    new ScalarNumeric[Double] {
      @inline def toDouble(v: Double): Double = v
    }
}
