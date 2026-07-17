/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.deequ.analyzers.catalyst.KLLSketchSerializer
import com.amazon.deequ.analyzers.{DataTypeHistogram, QuantileNonSample}
import com.google.common.primitives.Doubles
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.util.matching.Regex

/** Test-only copies of the legacy implementations used as differential oracles. */
private[sql] class LegacyStatefulDataType extends UserDefinedAggregateFunction {

  private val NULL_POS = 0
  private val FRACTIONAL_POS = 1
  private val INTEGRAL_POS = 2
  private val BOOLEAN_POS = 3
  private val STRING_POS = 4

  private val FRACTIONAL: Regex =
    """^(-|\+)? ?\d+((\.\d+)|((?:\.\d+)?[Ee][-+]?\d+))$""".r
  private val INTEGRAL: Regex = """^(-|\+)? ?\d+$""".r
  private val BOOLEAN: Regex = """^(true|false)$""".r

  override def inputSchema: StructType =
    StructType(StructField("value", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("null", LongType) ::
    StructField("fractional", LongType) ::
    StructField("integral", LongType) ::
    StructField("boolean", LongType) ::
    StructField("string", LongType) :: Nil)

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(NULL_POS) = 0L
    buffer(FRACTIONAL_POS) = 0L
    buffer(INTEGRAL_POS) = 0L
    buffer(BOOLEAN_POS) = 0L
    buffer(STRING_POS) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.isNullAt(0)) {
      buffer(NULL_POS) = buffer.getLong(NULL_POS) + 1L
    } else {
      input.getString(0) match {
        case FRACTIONAL(_*) => buffer(FRACTIONAL_POS) = buffer.getLong(FRACTIONAL_POS) + 1L
        case INTEGRAL(_*) => buffer(INTEGRAL_POS) = buffer.getLong(INTEGRAL_POS) + 1L
        case BOOLEAN(_*) => buffer(BOOLEAN_POS) = buffer.getLong(BOOLEAN_POS) + 1L
        case _ => buffer(STRING_POS) = buffer.getLong(STRING_POS) + 1L
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(NULL_POS) = buffer1.getLong(NULL_POS) + buffer2.getLong(NULL_POS)
    buffer1(FRACTIONAL_POS) = buffer1.getLong(FRACTIONAL_POS) + buffer2.getLong(FRACTIONAL_POS)
    buffer1(INTEGRAL_POS) = buffer1.getLong(INTEGRAL_POS) + buffer2.getLong(INTEGRAL_POS)
    buffer1(BOOLEAN_POS) = buffer1.getLong(BOOLEAN_POS) + buffer2.getLong(BOOLEAN_POS)
    buffer1(STRING_POS) = buffer1.getLong(STRING_POS) + buffer2.getLong(STRING_POS)
  }

  override def evaluate(buffer: Row): Any = {
    DataTypeHistogram.toBytes(
      buffer.getLong(NULL_POS),
      buffer.getLong(FRACTIONAL_POS),
      buffer.getLong(INTEGRAL_POS),
      buffer.getLong(BOOLEAN_POS),
      buffer.getLong(STRING_POS))
  }
}

private[sql] class LegacyStatefulKLLSketch(
    sketchSize: Int,
    shrinkingFactor: Double)
  extends UserDefinedAggregateFunction {

  private val OBJECT_POS = 0
  private val MIN_POS = 1
  private val MAX_POS = 2

  override def inputSchema: StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("data", BinaryType) ::
    StructField("minimum", DoubleType) ::
    StructField("maximum", DoubleType) :: Nil)

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val sketch = new QuantileNonSample[Double](sketchSize, shrinkingFactor)
    buffer(OBJECT_POS) = serialize(sketch)
    buffer(MIN_POS) = Int.MaxValue.toDouble
    buffer(MAX_POS) = Int.MinValue.toDouble
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(OBJECT_POS)) {
      val value = input.getDouble(OBJECT_POS)
      val sketch = deserialize(buffer.getAs[Array[Byte]](OBJECT_POS))
      sketch.update(value)
      buffer(OBJECT_POS) = serialize(sketch)
      buffer(MIN_POS) = Math.min(buffer.getDouble(MIN_POS), value)
      buffer(MAX_POS) = Math.max(buffer.getDouble(MAX_POS), value)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(OBJECT_POS)) {
      val sketch = deserialize(buffer1.getAs[Array[Byte]](OBJECT_POS))
      sketch.merge(deserialize(buffer2.getAs[Array[Byte]](OBJECT_POS)))
      buffer1(OBJECT_POS) = serialize(sketch)
      buffer1(MIN_POS) = Math.min(buffer1.getDouble(MIN_POS), buffer2.getDouble(MIN_POS))
      buffer1(MAX_POS) = Math.max(buffer1.getDouble(MAX_POS), buffer2.getDouble(MAX_POS))
    }
  }

  override def evaluate(buffer: Row): Any = {
    val sketch = buffer.getAs[Array[Byte]](OBJECT_POS)
    ByteBuffer.allocate(2 * Doubles.BYTES + sketch.length)
      .putDouble(buffer.getDouble(MIN_POS))
      .putDouble(buffer.getDouble(MAX_POS))
      .put(sketch)
      .array()
  }

  private def serialize(sketch: QuantileNonSample[Double]): Array[Byte] =
    KLLSketchSerializer.serializer.serialize(sketch)

  private def deserialize(bytes: Array[Byte]): QuantileNonSample[Double] =
    KLLSketchSerializer.serializer.deserialize(bytes)
}
