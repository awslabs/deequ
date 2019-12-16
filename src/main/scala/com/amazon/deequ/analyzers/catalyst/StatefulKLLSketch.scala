/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.apache.spark.sql

import java.nio.ByteBuffer

import com.amazon.deequ.analyzers.QuantileNonSample
import com.amazon.deequ.analyzers.catalyst.KLLSketchSerializer
import com.google.common.primitives.Doubles

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


private [sql] class StatefulKLLSketch(
    sketchSize: Int,
    shrinkingFactor: Double)
  extends UserDefinedAggregateFunction{

  val OBJECT_POS = 0
  val MIN_POS = 1
  val MAX_POS = 2

  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("data", BinaryType) ::
    StructField("minimum", DoubleType) :: StructField("maximum", DoubleType) :: Nil)

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val qsketch = new QuantileNonSample[Double](sketchSize, shrinkingFactor)
    buffer(OBJECT_POS) = serialize(qsketch)
    buffer(MIN_POS) = Int.MaxValue.toDouble
    buffer(MAX_POS) = Int.MinValue.toDouble
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.isNullAt(OBJECT_POS)) {
      return
    }

    val tmp = input.getDouble(OBJECT_POS)
    val kll = deserialize(buffer.getAs[Array[Byte]](OBJECT_POS))
    kll.update(tmp)
    buffer(OBJECT_POS) = serialize(kll)
    buffer(MIN_POS) = Math.min(buffer.getDouble(MIN_POS), tmp)
    buffer(MAX_POS) = Math.max(buffer.getDouble(MAX_POS), tmp)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer2.isNullAt(OBJECT_POS)) {
      return
    }

    val kll_this = deserialize(buffer1.getAs[Array[Byte]](OBJECT_POS))
    val kll_other = deserialize(buffer2.getAs[Array[Byte]](OBJECT_POS))
    val kll_ret = kll_this.merge(kll_other)
    buffer1(OBJECT_POS) = serialize(kll_ret)
    buffer1(MIN_POS) = Math.min(buffer1.getDouble(MIN_POS), buffer2.getDouble(MIN_POS))
    buffer1(MAX_POS) = Math.max(buffer1.getDouble(MAX_POS), buffer2.getDouble(MAX_POS))
  }

  override def evaluate(buffer: Row): Any = {
    toBytes(buffer.getDouble(MIN_POS),
      buffer.getDouble(MAX_POS),
      buffer.getAs[Array[Byte]](OBJECT_POS))
  }

  def toBytes(min: Double, max: Double, obj: Array[Byte]): Array[Byte] = {
    val buffer2 = ByteBuffer.wrap(new Array(Doubles.BYTES + Doubles.BYTES + obj.length))
    buffer2.putDouble(min)
    buffer2.putDouble(max)
    buffer2.put(obj)
    buffer2.array()
  }

  def serialize(obj: QuantileNonSample[Double]): Array[Byte] = {
    KLLSketchSerializer.serializer.serialize(obj)
  }

  def deserialize(bytes: Array[Byte]): QuantileNonSample[Double] = {
    KLLSketchSerializer.serializer.deserialize(bytes)
  }
}

