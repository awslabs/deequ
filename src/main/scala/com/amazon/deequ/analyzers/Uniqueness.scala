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

import com.amazon.deequ.analyzers.Analyzers.COUNT_COL
import com.amazon.deequ.analyzers.FilteredRowOutcome.FilteredRowOutcome
import com.amazon.deequ.metrics.DoubleMetric
import com.google.common.annotations.VisibleForTesting
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.not
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.DoubleType

/** Uniqueness is the fraction of unique values of a column(s), i.e.,
  * values that occur exactly once. */
case class Uniqueness(columns: Seq[String], where: Option[String] = None,
                      analyzerOptions: Option[AnalyzerOptions] = None)
  extends ScanShareableFrequencyBasedAnalyzer("Uniqueness", columns)
  with FilterableAnalyzer {

  override def aggregationFunctions(numRows: Long): Seq[Column] = {
   (sum(col(COUNT_COL).equalTo(lit(1)).cast(DoubleType)) / numRows) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int, fullColumn: Option[Column]): DoubleMetric = {
    val conditionColumn = where.map { expression => expr(expression) }
    val fullColumnUniqueness = fullColumn.map {
      rowLevelColumn => {
        conditionColumn.map {
          condition => {
            when(not(condition), getRowLevelFilterTreatment(analyzerOptions).getExpression)
              .when(rowLevelColumn.equalTo(1), true).otherwise(false)
          }
        }.getOrElse(when(rowLevelColumn.equalTo(1), true).otherwise(false))
      }
    }
    super.fromAggregationResult(result, offset, fullColumnUniqueness)
  }

  override def filterCondition: Option[String] = where
}

object Uniqueness {
  def apply(column: String): Uniqueness = {
    new Uniqueness(column :: Nil)
  }

  def apply(column: String, where: Option[String]): Uniqueness = {
    new Uniqueness(column :: Nil, where)
  }
}
