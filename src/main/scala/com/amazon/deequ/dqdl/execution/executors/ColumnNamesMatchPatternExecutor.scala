/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.deequ.dqdl.model.{ColumnNamesMatchPatternExecutableRule, Failed, Passed, RuleOutcome}
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.DQRule

import scala.util.matching.Regex

object ColumnNamesMatchPatternExecutor extends DQDLExecutor.RuleExecutor[ColumnNamesMatchPatternExecutableRule] {

  override def executeRules(rules: Seq[ColumnNamesMatchPatternExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    rules.map { rule =>
      val pattern = new Regex(rule.pattern)
      val columns = df.columns
      val columnCount = columns.length
      val unmatchedColumns = columns.filter { col => pattern.findAllMatchIn(col).isEmpty }
      val metricValue = (columnCount - unmatchedColumns.length).toDouble / columnCount.toDouble
      val evaluatedMetric = Map("Dataset.*.ColumnNamesPatternMatchRatio" -> metricValue)

      val outcome = if (unmatchedColumns.isEmpty) {
        RuleOutcome(rule.dqRule, Passed, None, evaluatedMetric)
      } else {
        RuleOutcome(rule.dqRule, Failed,
          Some(s"Columns that do not match the pattern: ${unmatchedColumns.mkString(", ")}"),
          evaluatedMetric)
      }

      rule.dqRule -> outcome
    }.toMap
  }
}
