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

import com.amazon.deequ.comparison.{ComparisonFailed, ReferentialIntegrity}
import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{Failed, Passed, ReferentialIntegrityExecutableRule, RuleOutcome}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import software.amazon.glue.dqdl.model.DQRule

object ReferentialIntegrityExecutor extends DQDLExecutor.RuleExecutor[ReferentialIntegrityExecutableRule] {

  override def executeRules(rules: Seq[ReferentialIntegrityExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    rules.map { rule =>
      rule.dqRule -> (additionalDataSources.get(rule.referenceDatasetAlias) match {
        case Some(referenceDF) =>
          val outcomeCol = java.util.UUID.randomUUID().toString
          ReferentialIntegrity.subsetCheckRowLevel(
            df, rule.primaryColumns, referenceDF, rule.referenceColumns, Some(outcomeCol)
          ) match {
            case Left(ComparisonFailed(errorMessage, _)) =>
              RuleOutcome(rule.dqRule, Failed, Some(errorMessage))
            case Right(augmentedDF) =>
              val filteredCount = augmentedDF.filter(col(outcomeCol) === lit(true)).count()
              val totalCount = df.count()
              val ratio = filteredCount.toDouble / totalCount
              val evaluatedMetrics = Map(s"Column.${rule.referenceDatasetAlias}.ReferentialIntegrity" -> ratio)

              if (rule.assertion(ratio)) {
                RuleOutcome(rule.dqRule, Passed, None, evaluatedMetrics)
              } else {
                RuleOutcome(rule.dqRule, Failed,
                  Some(s"Value: $ratio does not meet the constraint requirement."), evaluatedMetrics)
              }
          }
        case None =>
          RuleOutcome(rule.dqRule, Failed,
            Some(s"${rule.referenceDatasetAlias} not found in additional sources"))
      })
    }.toMap
  }
}
