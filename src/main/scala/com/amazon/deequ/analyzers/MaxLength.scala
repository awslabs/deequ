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

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.NullBehavior.NullBehavior
import com.amazon.deequ.analyzers.Preconditions.hasColumn
import com.amazon.deequ.analyzers.Preconditions.isString
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.not
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType

case class MaxLength(column: String, where: Option[String] = None, analyzerOptions: Option[AnalyzerOptions] = None)
  extends StandardScanShareableAnalyzer[MaxState]("MaxLength", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    max(criterion) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MaxState] = {
    ifNoNullsIn(result, offset) { _ =>
      MaxState(result.getDouble(offset), Some(rowLevelResults))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column):: isString(column) :: Nil
  }

  override def filterCondition: Option[String] = where

  private[deequ] def criterion: Column = {
    transformColForNullBehavior
  }

  private[deequ] def rowLevelResults: Column = {
    transformColForFilteredRow(criterion)
  }

  private def transformColForFilteredRow(col: Column): Column = {
    val whereNotCondition = where.map { expression => not(expr(expression)) }
    getRowLevelFilterTreatment(analyzerOptions) match {
      case FilteredRowOutcome.TRUE =>
        conditionSelectionGivenColumn(col, whereNotCondition, replaceWith = Double.MinValue)
      case _ =>
        conditionSelectionGivenColumn(col, whereNotCondition, replaceWith = null)
    }
  }

  private def transformColForNullBehavior: Column = {
    val isNullCheck = col(column).isNull
    val colLengths: Column = length(conditionalSelection(column, where)).cast(DoubleType)
    getNullBehavior match {
      case NullBehavior.Fail =>
        conditionSelectionGivenColumn(colLengths, Option(isNullCheck), replaceWith = Double.MaxValue)
      case NullBehavior.EmptyString =>
        // Empty String is 0 length string
        conditionSelectionGivenColumn(colLengths, Option(isNullCheck), replaceWith = 0.0).cast(DoubleType)
      case _ =>
        colLengths
    }
  }

  private def getNullBehavior: NullBehavior = {
    analyzerOptions
      .map { options => options.nullBehavior }
      .getOrElse(NullBehavior.Ignore)
  }
}
