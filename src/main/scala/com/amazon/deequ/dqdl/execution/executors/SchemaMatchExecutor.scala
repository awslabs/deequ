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

import com.amazon.deequ.comparison.{ComparisonFailed, ComparisonSucceeded, SchemaMatch}
import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{Passed, Failed, RuleOutcome, SchemaMatchExecutableRule}
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.DQRule

object SchemaMatchExecutor extends DQDLExecutor.RuleExecutor[SchemaMatchExecutableRule] {

  override def executeRules(rules: Seq[SchemaMatchExecutableRule],
                            df: DataFrame,
                            additionalDataSources: Map[String, DataFrame]): Map[DQRule, RuleOutcome] = {
    rules.map { rule =>
      rule.dqRule -> (additionalDataSources.get(rule.referenceDatasetAlias) match {
        case Some(referenceDF) =>
          SchemaMatch.matchSchema(df, referenceDF, rule.assertion) match {
            case ComparisonSucceeded(ratio) =>
              RuleOutcome(rule.dqRule, Passed, None,
                Map(s"Dataset.${rule.referenceDatasetAlias}.SchemaMatch" -> ratio))
            case ComparisonFailed(msg, ratio) =>
              RuleOutcome(rule.dqRule, Failed, Some(msg),
                Map(s"Dataset.${rule.referenceDatasetAlias}.SchemaMatch" -> ratio))
          }
        case None =>
          RuleOutcome(rule.dqRule, Failed,
            Some(s"Reference dataset '${rule.referenceDatasetAlias}' not found"))
      })
    }.toMap
  }
}
