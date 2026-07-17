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

import org.apache.spark.sql.expressions.Aggregator

private[sql] final class KLLAggregationBuffer() extends Serializable {

  private var qSketch: QuantileNonSample[Double] = _
  private var minimum: Double = _
  private var maximum: Double = _

  def this(qSketch: QuantileNonSample[Double], minimum: Double, maximum: Double) = {
    this()
    this.qSketch = qSketch
    this.minimum = minimum
    this.maximum = maximum
  }

  private[sql] def sketch: QuantileNonSample[Double] = qSketch

  def getSerializedSketch: Array[Byte] = KLLSketchSerializer.serializer.serialize(qSketch)

  def setSerializedSketch(bytes: Array[Byte]): Unit = {
    qSketch = KLLSketchSerializer.serializer.deserialize(bytes)
  }

  def getMinimum: Double = minimum

  def setMinimum(value: Double): Unit = {
    minimum = value
  }

  def getMaximum: Double = maximum

  def setMaximum(value: Double): Unit = {
    maximum = value
  }
}

private[sql] class StatefulKLLSketch(
    sketchSize: Int,
    shrinkingFactor: Double)
  extends Aggregator[java.lang.Double, KLLAggregationBuffer, Array[Byte]] {

  override def zero: KLLAggregationBuffer = {
    new KLLAggregationBuffer(
      new QuantileNonSample[Double](sketchSize, shrinkingFactor),
      Int.MaxValue.toDouble,
      Int.MinValue.toDouble)
  }

  override def reduce(
      buffer: KLLAggregationBuffer,
      input: java.lang.Double)
    : KLLAggregationBuffer = {

    if (input != null) {
      val value = input.doubleValue()
      buffer.sketch.update(value)
      buffer.setMinimum(Math.min(buffer.getMinimum, value))
      buffer.setMaximum(Math.max(buffer.getMaximum, value))
    }
    buffer
  }

  override def merge(
      buffer1: KLLAggregationBuffer,
      buffer2: KLLAggregationBuffer)
    : KLLAggregationBuffer = {

    buffer1.sketch.merge(buffer2.sketch)
    buffer1.setMinimum(Math.min(buffer1.getMinimum, buffer2.getMinimum))
    buffer1.setMaximum(Math.max(buffer1.getMaximum, buffer2.getMaximum))
    buffer1
  }

  override def finish(buffer: KLLAggregationBuffer): Array[Byte] = {
    toBytes(buffer.getMinimum, buffer.getMaximum, serialize(buffer.sketch))
  }

  override def bufferEncoder: Encoder[KLLAggregationBuffer] =
    Encoders.bean(classOf[KLLAggregationBuffer])

  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  private def toBytes(min: Double, max: Double, obj: Array[Byte]): Array[Byte] = {
    val buffer2 = ByteBuffer.wrap(new Array(Doubles.BYTES + Doubles.BYTES + obj.length))
    buffer2.putDouble(min)
    buffer2.putDouble(max)
    buffer2.put(obj)
    buffer2.array()
  }

  private def serialize(obj: QuantileNonSample[Double]): Array[Byte] = {
    KLLSketchSerializer.serializer.serialize(obj)
  }
}
