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

package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{col, element_at, min}
import org.apache.spark.sql.types.{DoubleType, StructType}
import Analyzers._
import com.amazon.deequ.metrics.FullColumn
import com.google.common.annotations.VisibleForTesting

case class MinState(minValue: Double, override val fullColumn: Option[Column] = None)
  extends DoubleValuedState[MinState] with FullColumn {

  override def sum(other: MinState): MinState = {
    MinState(math.min(minValue, other.minValue), sum(fullColumn, other.fullColumn))
  }

  override def metricValue(): Double = {
    minValue
  }
}

case class Minimum(column: String, where: Option[String] = None, analyzerOptions: Option[AnalyzerOptions] = None)
  extends StandardScanShareableAnalyzer[MinState]("Minimum", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    min(element_at(criterion, 2).cast(DoubleType)) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MinState] = {
    ifNoNullsIn(result, offset) { _ =>
      MinState(result.getDouble(offset), Some(criterion))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where

  @VisibleForTesting
  private def criterion: Column = conditionalSelectionWithAugmentedOutcome(col(column), where, Double.MaxValue)
}
