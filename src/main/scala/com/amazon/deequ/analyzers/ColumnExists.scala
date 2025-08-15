/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.apache.spark.sql.DataFrame

case class ColumnExistsState(exists: Boolean, column: String) extends DoubleValuedState[ColumnExistsState] {
  override def sum(other: ColumnExistsState): ColumnExistsState = this
  
  override def metricValue(): Double = if (exists) 1.0 else 0.0
}

case class ColumnExists(column: String) extends Analyzer[ColumnExistsState, DoubleMetric] {

  val name = "ColumnExists"
  val instance: String = column
  val entity: Entity.Value = Entity.Dataset

  /**
   * Compute the state (sufficient statistics) from the data
   *
   * @param data the input dataframe
   * @return 1 if the column exists, 0 otherwise
   */
  override def computeStateFrom(data: DataFrame, filterCondition: Option[String]): Option[ColumnExistsState] = {
    if (filterCondition.isDefined) {
      throw new IllegalArgumentException("ColumnExists does not accept a filter condition")
    } else {
      val exists = data.columns.contains(column)
      Some(ColumnExistsState(exists, column))
    }
  }

  /**
   * Compute the metric from the state (sufficient statistics)
   *
   * @param state the computed state from [[computeStateFrom]]
   * @return a double metric indicating whether the column exists (1.0) or not (0.0)
   */
  override def computeMetricFrom(state: Option[ColumnExistsState]): DoubleMetric = state match {
    case Some(s) => Analyzers.metricFromValue(s.metricValue(), name, instance, entity)
    case None => Analyzers.metricFromEmpty(this, name, instance, entity)
  }

  /**
   * Compute the metric from a failure
   */
  override private[deequ] def toFailureMetric(failure: Exception): DoubleMetric = {
    Analyzers.metricFromFailure(failure, name, instance, entity)
  }
}
