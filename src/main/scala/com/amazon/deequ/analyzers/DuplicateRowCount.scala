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

import com.amazon.deequ.analyzers.Analyzers.COUNT_COL
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.not
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType

/** DuplicateRowCount counts the number of rows that appear more than once
  * across the specified columns. If columns is empty, all columns are used. */
case class DuplicateRowCount(columns: Seq[String], where: Option[String] = None,
                             analyzerOptions: Option[AnalyzerOptions] = None)
  extends ScanShareableFrequencyBasedAnalyzer("DuplicateRowCount", columns)
  with FilterableAnalyzer {

  override def aggregationFunctions(numRows: Long): Seq[Column] = {
    sum(when(col(COUNT_COL) > lit(1), col(COUNT_COL)).otherwise(lit(0)).cast(DoubleType)) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int, fullColumn: Option[Column]): DoubleMetric = {
    if (result.isNullAt(offset)) {
      // WHERE clause matched zero rows — return 0 (no duplicates)
      toSuccessMetric(0.0, fullColumn)
    } else {
      val conditionColumn = where.map { expression => expr(expression) }
      val fullColumnDuplicate = fullColumn.map { rowLevelColumn =>
        conditionColumn.map { condition =>
          when(not(condition), getRowLevelFilterTreatment(analyzerOptions).getExpression)
            .when(rowLevelColumn > lit(1), true).otherwise(false)
        }.getOrElse(when(rowLevelColumn > lit(1), true).otherwise(false))
      }
      super.fromAggregationResult(result, offset, fullColumnDuplicate)
    }
  }

  override def computeStateFrom(data: DataFrame,
                                filterCondition: Option[String] = None): Option[FrequenciesAndNumRows] = {
    if (columns.isEmpty) {
      val allColumns = data.columns.toSeq
      DuplicateRowCount(allColumns, where, analyzerOptions).computeStateFrom(data, filterCondition)
    } else {
      super.computeStateFrom(data, filterCondition)
    }
  }

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): DoubleMetric = {
    state match {
      case Some(theState) if theState.numRows == 0 =>
        // Empty data or WHERE matched nothing — 0 duplicates
        toSuccessMetric(0.0, theState.fullColumn)
      case _ =>
        super.computeMetricFrom(state)
    }
  }

  /** For empty columns, resolve all DataFrame columns and re-wrap metric entity */
  override def calculate(
      data: DataFrame,
      aggregateWith: Option[StateLoader] = None,
      saveStatesWith: Option[StatePersister] = None,
      filterCondition: Option[String] = None): DoubleMetric = {
    val effectiveFilter = filterCondition.orElse(where)
    if (columns.isEmpty) {
      val resolved = DuplicateRowCount(data.columns.toSeq, None, analyzerOptions)
      val metric = resolved.calculate(data, aggregateWith, saveStatesWith, effectiveFilter)
      // Re-wrap with Dataset.* entity to match DQDL metric mapping
      metric.copy(entity = Entity.Dataset, instance = "*")
    } else {
      super.calculate(data, aggregateWith, saveStatesWith, effectiveFilter)
    }
  }

  override def preconditions: Seq[StructType => Unit] = {
    if (columns.isEmpty) {
      Seq.empty
    } else {
      super.preconditions
    }
  }

  override def filterCondition: Option[String] = where

  override def columnsReferenced(): Option[Set[String]] =
    if (columns.isEmpty || where.isDefined) None else Some(columns.toSet)
}

object DuplicateRowCount {
  def apply(column: String): DuplicateRowCount = {
    new DuplicateRowCount(column :: Nil)
  }

  def apply(column: String, where: Option[String]): DuplicateRowCount = {
    new DuplicateRowCount(column :: Nil, where)
  }
}
