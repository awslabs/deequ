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

import com.amazon.deequ.comparison.{ComparisonFailed, ComparisonSucceeded, RowCountMatch}
import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{Failed, Passed, RowCountMatchExecutableRule, RuleOutcome}
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.DQRule

object RowCountMatchExecutor extends DQDLExecutor.RuleExecutor[RowCountMatchExecutableRule] {

  override def executeRules(rules: Seq[RowCountMatchExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    rules.map { rule =>
      rule.dqRule -> (additionalDataSources.get(rule.referenceDatasetAlias) match {
        case Some(referenceDF) =>
          RowCountMatch.matchRowCounts(df, referenceDF, rule.assertion) match {
            case ComparisonSucceeded(ratio) =>
              RuleOutcome(rule.dqRule, Passed, None,
                Map(s"Dataset.${rule.referenceDatasetAlias}.RowCountMatch" -> ratio))
            case ComparisonFailed(msg, ratio) =>
              RuleOutcome(rule.dqRule, Failed, Some(msg),
                Map(s"Dataset.${rule.referenceDatasetAlias}.RowCountMatch" -> ratio))
          }
        case None =>
          RuleOutcome(rule.dqRule, Failed,
            Some(s"Reference dataset '${rule.referenceDatasetAlias}' not found in additional data sources"))
      })
    }.toMap
  }
}
