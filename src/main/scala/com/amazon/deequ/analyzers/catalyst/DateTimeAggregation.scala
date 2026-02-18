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

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import java.time.Instant


private[sql] class DateTimeAggregation(
  frequency: Long
) extends Aggregator[Instant, Map[Long, Long], Map[Long, Long]] {
  override def zero: Map[Long, Long] = Map.empty[Long, Long]

  override def reduce(agg: Map[Long, Long], input: Instant): Map[Long, Long] = {
    val dateTime = input.toEpochMilli
    val batchTime = dateTime - (dateTime % frequency)
    agg + (batchTime -> (agg.getOrElse(batchTime, 0L) + 1L))
  }

  override def merge(b1: Map[Long, Long], b2: Map[Long, Long]): Map[Long, Long] = {
    b1 ++ b2.map {
      case (k, v) => k -> (v + b1.getOrElse(k, 0L))
    }
  }

  override def finish(reduction: Map[Long, Long]): Map[Long, Long] = reduction

  // Define encoder for buffer
  def bufferEncoder: Encoder[Map[Long, Long]] = ExpressionEncoder()

  // Define encoder for output
  def outputEncoder: Encoder[Map[Long, Long]] = ExpressionEncoder()
}