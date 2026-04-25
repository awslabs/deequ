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

package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import com.amazon.deequ.analyzers.Analyzers._
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.types.{DoubleType, StructType}

case class RangeState(
    minValue: Double,
    maxValue: Double)
  extends DoubleValuedState[RangeState] {

  override def metricValue(): Double = {
    maxValue - minValue
  }

  override def sum(other: RangeState): RangeState = {
    RangeState(math.min(minValue, other.minValue), math.max(maxValue, other.maxValue))
  }
}

case class Range(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[RangeState]("Range", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    min(conditionalSelection(column, where)).cast(DoubleType) ::
      max(conditionalSelection(column, where)).cast(DoubleType) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[RangeState] = {
    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      RangeState(result.getDouble(offset), result.getDouble(offset + 1))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}
