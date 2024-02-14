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

import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNotNested}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{IntegerType, StructType}
import Analyzers._
import com.amazon.deequ.analyzers.FilteredRow.FilteredRow
import com.google.common.annotations.VisibleForTesting
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, Row}

/** Completeness is the fraction of non-null values in a column of a DataFrame. */
case class Completeness(column: String, where: Option[String] = None,
                        analyzerOptions: Option[AnalyzerOptions] = None) extends
  StandardScanShareableAnalyzer[NumMatchesAndCount]("Completeness", column) with
  FilterableAnalyzer {

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatchesAndCount] = {
    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1), Some(rowLevelResults))
    }
  }

  override def aggregationFunctions(): Seq[Column] = {

    val summation = sum(criterion.cast(IntegerType))

    summation :: conditionalCount(where) :: Nil
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNotNested(column) :: Nil
  }

  override def filterCondition: Option[String] = where

  @VisibleForTesting // required by some tests that compare analyzer results to an expected state
  private[deequ] def criterion: Column = conditionalSelection(column, where).isNotNull

  @VisibleForTesting
  private[deequ] def rowLevelResults: Column = {
    val whereCondition = where.map { expression => expr(expression)}
    conditionalSelectionFilteredFromColumns(col(column).isNotNull, whereCondition, getRowLevelFilterTreatment.toString)
  }

  private def getRowLevelFilterTreatment: FilteredRow = {
    analyzerOptions
      .map { options => options.filteredRow }
      .getOrElse(FilteredRow.TRUE)
  }
}
