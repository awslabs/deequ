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
import com.amazon.deequ.dqdl.model.{DataFreshnessExecutableRule, RuleOutcome}
import com.amazon.deequ.dqdl.util.RuleEvaluationHelper
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.DQRule

object DataFreshnessExecutor extends DQDLExecutor.RuleExecutor[DataFreshnessExecutableRule] {

  override def executeRules(rules: Seq[DataFreshnessExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    rules.map { rule =>
      val metricName = s"Column.${rule.column}.DataFreshness.Compliance"
      val result = RuleEvaluationHelper.evaluateRuleWithRowLevel(
        df,
        rule.dqRule,
        rule.column,
        rule.filteredRow,
        metricName,
        rule.outcomeExpression,
        None
      )
      rule.dqRule -> result.outcome
    }.toMap
  }
}
