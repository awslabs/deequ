/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.dqdl.translation.rules

import com.amazon.deequ.analyzers.FilteredRowOutcome
import com.amazon.deequ.dqdl.model.ColumnValuesDateExecutableRule
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, to_date}
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.date.{DateBasedCondition, DateBasedConditionOperator}

import scala.collection.JavaConverters._

object ColumnValuesDateRule {

  def toExecutableRule(rule: DQRule, filteredRowOutcome: FilteredRowOutcome.FilteredRowOutcome):
    Either[String, ColumnValuesDateExecutableRule] = {
    val params = rule.getParameters.asScala
    if (!params.contains("TargetColumn")) {
      return Left("TargetColumn parameter is required for ColumnValues rule")
    }
    val columnName = params("TargetColumn").replaceAll("\"", "")

    rule.getCondition match {
      case condition: DateBasedCondition =>
        val expr = dateConditionToColumnExpression(columnName, condition)
        Right(ColumnValuesDateExecutableRule(rule, columnName, expr, filteredRowOutcome))
      case _ =>
        Left("Unsupported condition type for ColumnValues date rule")
    }
  }

  private def dateConditionToColumnExpression(targetColumn: String,
                                              condition: DateBasedCondition): Column = {
    val localDateTimeDateLength = "2023-11-11".length
    val evaluatedOperands = condition.getOperands.asScala
      .map(_.getEvaluatedExpression)
      .map(_.toString.substring(0, localDateTimeDateLength))

    val dateColumn: Column = to_date(col(targetColumn))

    condition.getOperator match {
      case DateBasedConditionOperator.BETWEEN =>
        (dateColumn > lit(evaluatedOperands.head)) && (dateColumn < lit(evaluatedOperands.last))
      case DateBasedConditionOperator.NOT_BETWEEN =>
        (dateColumn <= lit(evaluatedOperands.head)) || (dateColumn >= lit(evaluatedOperands.last))
      case DateBasedConditionOperator.GREATER_THAN_EQUAL_TO =>
        dateColumn >= lit(evaluatedOperands.head)
      case DateBasedConditionOperator.GREATER_THAN =>
        dateColumn > lit(evaluatedOperands.head)
      case DateBasedConditionOperator.LESS_THAN_EQUAL_TO =>
        dateColumn <= lit(evaluatedOperands.head)
      case DateBasedConditionOperator.LESS_THAN =>
        dateColumn < lit(evaluatedOperands.head)
      case DateBasedConditionOperator.EQUALS =>
        dateColumn === lit(evaluatedOperands.head)
      case DateBasedConditionOperator.NOT_EQUALS =>
        dateColumn =!= lit(evaluatedOperands.head)
      case DateBasedConditionOperator.IN =>
        dateColumn.isin(evaluatedOperands: _*)
      case DateBasedConditionOperator.NOT_IN =>
        !dateColumn.isin(evaluatedOperands: _*)
    }
  }
}
