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

package com.amazon.deequ.dqdl.execution.executors

import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{ColumnDataTypeExecutableRule, RuleOutcome, SingularColumn}
import com.amazon.deequ.dqdl.util.RuleEvaluationHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import software.amazon.glue.dqdl.model.DQRule

case class ColumnDataTypeExecutionResult(
  outcomes: Map[DQRule, RuleOutcome], rowLevelData: DataFrame)

object ColumnDataTypeExecutor
    extends DQDLExecutor.RuleExecutor[ColumnDataTypeExecutableRule] {

  override def executeRules(
      rules: Seq[ColumnDataTypeExecutableRule],
      df: DataFrame,
      additionalDataSources: Map[String, DataFrame] = Map.empty
  ): Map[DQRule, RuleOutcome] = {
    executeWithRowLevel(rules, df).outcomes
  }

  def executeWithRowLevel(
      rules: Seq[ColumnDataTypeExecutableRule],
      df: DataFrame,
      baseRowLevelData: Option[DataFrame] = None
  ): ColumnDataTypeExecutionResult = {
    if (rules.isEmpty) {
      return ColumnDataTypeExecutionResult(
        Map.empty, baseRowLevelData.getOrElse(df))
    }

    var currentData = baseRowLevelData.getOrElse(df)
    val outcomes = rules.map { rule =>
      val metricName =
        s"Column.${rule.column}.ColumnDataType.Compliance"
      val colName = java.util.UUID.randomUUID().toString
      val outcome = RuleEvaluationHelper.evaluateRuleAgainstColumn(
        df, rule.dqRule, rule.column, rule.filteredRow,
        metricName, rule.outcomeExpression, Some(rule.assertion))
      val hasMetrics = outcome.evaluatedMetrics.nonEmpty
      currentData = if (hasMetrics) {
        Option(rule.dqRule.getWhereClause) match {
          case Some(where) =>
            currentData.withColumn(colName,
              when(not(expr(where)), null)
                .otherwise(rule.outcomeExpression))
          case None =>
            currentData.withColumn(colName, rule.outcomeExpression)
        }
      } else {
        currentData.withColumn(colName, lit(false))
      }
      rule.dqRule -> outcome.copy(
        rowLevelOutcome = SingularColumn(colName))
    }
    ColumnDataTypeExecutionResult(outcomes.toMap, currentData)
  }
}
