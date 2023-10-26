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
import com.amazon.deequ.analyzers.Analyzers.{conditionalSelection, ifNoNullsIn}
import com.amazon.deequ.metrics.FullColumn
import org.apache.curator.shaded.com.google.common.annotations.VisibleForTesting
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DoubleType, StructType}

case class ExactQuantileState(exactQuantile: Double, quantile: Double, override val fullColumn: Option[Column] = None)
extends DoubleValuedState[ExactQuantileState] with FullColumn {
  override def sum(other: ExactQuantileState): ExactQuantileState = {

    ExactQuantileState(
      expr(s"percentile($fullColumn, $quantile)").toString().toDouble,
      quantile,
      sum(fullColumn, other.fullColumn))
  }

  override def metricValue(): Double = {
    exactQuantile
  }
}

case class ExactQuantile(column: String,
                         quantile: Double,
                         where: Option[String] = None)
extends StandardScanShareableAnalyzer[ExactQuantileState]("ExactQuantile", column)
with FilterableAnalyzer {
  override def aggregationFunctions(): Seq[Column] = {
    expr(s"percentile(${conditionalSelection(column, where).cast(DoubleType)}, $quantile)") :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[ExactQuantileState] = {
    ifNoNullsIn(result, offset) { _ =>
      ExactQuantileState(result.getDouble(offset), quantile, Some(criterion))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where

  @VisibleForTesting
  private def criterion: Column = conditionalSelection(column, where).cast(DoubleType)
}
