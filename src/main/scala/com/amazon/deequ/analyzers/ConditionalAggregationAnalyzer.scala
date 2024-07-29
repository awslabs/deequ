/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.deequ.analyzers

import com.amazon.deequ.metrics.AttributeDoubleMetric
import com.amazon.deequ.metrics.Entity
import org.apache.spark.sql.DataFrame

import scala.util.Failure
import scala.util.Success
import scala.util.Try

// Define a custom state to hold aggregation results
case class AggregatedMetricState(counts: Map[String, Int], totalRows: Int)
  extends DoubleValuedState[AggregatedMetricState] {

  def sum(other: AggregatedMetricState): AggregatedMetricState = {
    val combinedCounts = counts ++ other
      .counts
      .map { case (k, v) => k -> (v + counts.getOrElse(k, 0)) }
    AggregatedMetricState(combinedCounts, totalRows + other.totalRows)
  }

  def metricValue(): Double = counts.values.sum.toDouble / totalRows
}

// Define the analyzer
case class ConditionalAggregationAnalyzer(aggregatorFunc: DataFrame => AggregatedMetricState,
                                          metricName: String,
                                          instance: String)
  extends Analyzer[AggregatedMetricState, AttributeDoubleMetric] {

  def computeStateFrom(data: DataFrame, filterCondition: Option[String] = None)
  : Option[AggregatedMetricState] = {
    Try(aggregatorFunc(data)) match {
      case Success(state) => Some(state)
      case Failure(_) => None
    }
  }

  def computeMetricFrom(state: Option[AggregatedMetricState]): AttributeDoubleMetric = {
    state match {
      case Some(detState) =>
        val metrics = detState.counts.map { case (key, count) =>
          key -> (count.toDouble / detState.totalRows)
        }
        AttributeDoubleMetric(Entity.Column, metricName, instance, Success(metrics))
      case None =>
        AttributeDoubleMetric(Entity.Column, metricName, instance,
          Failure(new RuntimeException("Metric computation failed")))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception): AttributeDoubleMetric = {
    AttributeDoubleMetric(Entity.Column, metricName, instance, Failure(failure))
  }
}
