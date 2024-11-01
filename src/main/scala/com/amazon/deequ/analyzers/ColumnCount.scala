/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 *
 */

package com.amazon.deequ.analyzers

import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import org.apache.spark.sql.DataFrame

case class ColumnCount() extends Analyzer[NumMatches, DoubleMetric] {

  val name = "ColumnCount"
  val instance = "*"
  val entity = Entity.Dataset

  /**
   * Compute the state (sufficient statistics) from the data
   *
   * @param data the input dataframe
   * @return the number of columns in the input
   */
  override def computeStateFrom(data: DataFrame, filterCondition: Option[String]): Option[NumMatches] = {
    if (filterCondition.isDefined) {
      throw new IllegalArgumentException("ColumnCount does not accept a filter condition")
    } else {
      val numColumns = data.columns.size
      Some(NumMatches(numColumns))
    }
  }

  /**
   * Compute the metric from the state (sufficient statistics)
   *
   * @param state the computed state from [[computeStateFrom]]
   * @return a double metric indicating the number of columns for this analyzer
   */
  override def computeMetricFrom(state: Option[NumMatches]): DoubleMetric = {
    state
      .map(v => Analyzers.metricFromValue(v.metricValue(), name, instance, entity))
      .getOrElse(Analyzers.metricFromEmpty(this, name, instance, entity))
  }

  /**
   * Compute the metric from a failure - reports the exception thrown while trying to count columns
   */
  override private[deequ] def toFailureMetric(failure: Exception): DoubleMetric = {
    Analyzers.metricFromFailure(failure, name, instance, entity)
  }
}
