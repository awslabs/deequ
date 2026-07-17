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

import com.amazon.deequ.analyzers.DataTypeHistogram
import org.apache.spark.sql.expressions.Aggregator

import scala.util.matching.Regex

private[sql] final case class DataTypeAggregationBuffer(
    var numNull: Long,
    var numFractional: Long,
    var numIntegral: Long,
    var numBoolean: Long,
    var numString: Long)

private[sql] class StatefulDataType
  extends Aggregator[String, DataTypeAggregationBuffer, Array[Byte]] {

  val FRACTIONAL: Regex = """^(-|\+)? ?\d+((\.\d+)|((?:\.\d+)?[Ee][-+]?\d+))$""".r
  val INTEGRAL: Regex = """^(-|\+)? ?\d+$""".r
  val BOOLEAN: Regex = """^(true|false)$""".r

  override def zero: DataTypeAggregationBuffer = {
    DataTypeAggregationBuffer(0L, 0L, 0L, 0L, 0L)
  }

  override def reduce(
      buffer: DataTypeAggregationBuffer,
      input: String)
    : DataTypeAggregationBuffer = {

    if (input == null) {
      buffer.numNull += 1L
    } else {
      input match {
        case FRACTIONAL(_*) => buffer.numFractional += 1L
        case INTEGRAL(_*) => buffer.numIntegral += 1L
        case BOOLEAN(_*) => buffer.numBoolean += 1L
        case _ => buffer.numString += 1L
      }
    }
    buffer
  }

  override def merge(
      buffer1: DataTypeAggregationBuffer,
      buffer2: DataTypeAggregationBuffer)
    : DataTypeAggregationBuffer = {

    buffer1.numNull += buffer2.numNull
    buffer1.numFractional += buffer2.numFractional
    buffer1.numIntegral += buffer2.numIntegral
    buffer1.numBoolean += buffer2.numBoolean
    buffer1.numString += buffer2.numString
    buffer1
  }

  override def finish(buffer: DataTypeAggregationBuffer): Array[Byte] = {
    DataTypeHistogram.toBytes(
      buffer.numNull,
      buffer.numFractional,
      buffer.numIntegral,
      buffer.numBoolean,
      buffer.numString)
  }

  override def bufferEncoder: Encoder[DataTypeAggregationBuffer] =
    Encoders.product[DataTypeAggregationBuffer]

  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}
