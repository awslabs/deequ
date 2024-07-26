/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.deequ.analyzers

import com.amazon.deequ.metrics.EntityKeyedDoubleMetric
import com.amazon.deequ.metrics.Entity
import org.apache.spark.sql.DataFrame

import scala.util.Failure
import scala.util.Success
import scala.util.Try

// Define a custom state to hold detection results
case class EntityDetectionState(entityCounts: Map[String, Int], totalRows: Int)
  extends DoubleValuedState[EntityDetectionState] {

  def sum(other: EntityDetectionState): EntityDetectionState = {
    val combinedCounts = entityCounts ++ other
      .entityCounts
      .map { case (k, v) => k -> (v + entityCounts.getOrElse(k, 0)) }
    EntityDetectionState(combinedCounts, totalRows + other.totalRows)
  }

  def metricValue(): Double = entityCounts.values.sum.toDouble / totalRows
}

// Define the analyzer
case class EntityDetectionAnalyzer(lambda: DataFrame => EntityDetectionState, column: String)
  extends Analyzer[EntityDetectionState, EntityKeyedDoubleMetric] {

  def computeStateFrom(data: DataFrame, filterCondition: Option[String] = None): Option[EntityDetectionState] = {
    Try(lambda(data)) match {
      case Success(state) => Some(state)
      case Failure(_) => None
    }
  }

  def computeMetricFrom(state: Option[EntityDetectionState]): EntityKeyedDoubleMetric = {
    state match {
      case Some(detState) =>
        val metrics = detState.entityCounts.map { case (entity, count) =>
          entity -> (count.toDouble / detState.totalRows)
        }
        EntityKeyedDoubleMetric(Entity.Column, "EntityTypes", column, Success(metrics))
      case None =>
        EntityKeyedDoubleMetric(Entity.Column, "EntityTypes", column,
          Failure(new RuntimeException("Entity detection failed")))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception): EntityKeyedDoubleMetric = {
    EntityKeyedDoubleMetric(Entity.Column, "EntityTypes", column, Failure(failure))
  }
}
