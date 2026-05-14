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
import com.amazon.deequ.dqdl.model.{ColumnDataTypeExecutableRule, RuleOutcome}
import com.amazon.deequ.dqdl.util.RuleEvaluationHelper
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.DQRule

case class ColumnDataTypeExecutionResult(outcomes: Map[DQRule, RuleOutcome], rowLevelData: DataFrame)

object ColumnDataTypeExecutor extends DQDLExecutor.RuleExecutor[ColumnDataTypeExecutableRule] {

  override def executeRules(rules: Seq[ColumnDataTypeExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    executeWithRowLevel(rules, df).outcomes
  }

  def executeWithRowLevel(rules: Seq[ColumnDataTypeExecutableRule], df: DataFrame,
                          baseRowLevelData: Option[DataFrame] = None): ColumnDataTypeExecutionResult = {
    if (rules.isEmpty) {
      return ColumnDataTypeExecutionResult(Map.empty, baseRowLevelData.getOrElse(df))
    }

    var currentData = baseRowLevelData.getOrElse(df)
    val outcomes = rules.map { rule =>
      val metricName = s"Column.${rule.column}.ColumnDataType.Compliance"
      val result = RuleEvaluationHelper.evaluateRuleAgainstColumnWithRowLevel(
        currentData,
        rule.dqRule,
        rule.column,
        rule.filteredRow,
        metricName,
        rule.outcomeExpression,
        Some(rule.assertion)
      )
      currentData = result.augmentedData
      rule.dqRule -> result.outcome
    }
    ColumnDataTypeExecutionResult(outcomes.toMap, currentData)
  }
}
