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


import com.amazon.deequ.analyzers.KLLSketch
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, StatefulApproxQuantile, StatefulHyperloglogPlus}
import org.apache.spark.sql.catalyst.expressions.Literal

/* Custom aggregation functions used internally by deequ */
object DeequFunctions {

  private[this] def withAggregateFunction(
      func: AggregateFunction,
      isDistinct: Boolean = false): Column = {

    Column(func.toAggregateExpression(isDistinct))
  }

  /** Pearson correlation with state */
  def stateful_corr(columnA: String, columnB: String): Column = {
    stateful_corr(Column(columnA), Column(columnB))
  }

  /** Pearson correlation with state */
  def stateful_corr(columnA: Column, columnB: Column): Column = withAggregateFunction {
    new StatefulCorrelation(columnA.expr, columnB.expr)
  }

  /** Standard deviation with state */
  def stateful_stddev_pop(column: String): Column = {
    stateful_stddev_pop(Column(column))
  }

  /** Standard deviation with state */
  def stateful_stddev_pop(column: Column): Column = withAggregateFunction {
    StatefulStdDevPop(column.expr)
  }

  /** Approximate number of distinct values with state via HLL's */
  def stateful_approx_count_distinct(column: String): Column = {
    stateful_approx_count_distinct(Column(column))
  }

  /** Approximate number of distinct values with state via HLL's */
  def stateful_approx_count_distinct(column: Column): Column = withAggregateFunction {
    StatefulHyperloglogPlus(column.expr)
  }

  def stateful_approx_quantile(
      column: Column,
      relativeError: Double)
    : Column = withAggregateFunction {

    StatefulApproxQuantile(
      column.expr,
      // val relativeError = 1.0D / accuracy inside StatefulApproxQuantile
      Literal(1.0 / relativeError),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0
    )
  }

  /** Data type detection with state */
  def stateful_datatype(column: Column): Column = {
    val statefulDataType = new StatefulDataType()
    statefulDataType(column)
  }

  def stateful_kll(
      column: Column,
      sketchSize: Int,
      shrinkingFactor: Double): Column = {
    val statefulKLL = new StatefulKLLSketch(sketchSize, shrinkingFactor)
    statefulKLL(column)
  }
}


