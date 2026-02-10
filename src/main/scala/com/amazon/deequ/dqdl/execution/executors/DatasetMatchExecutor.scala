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

import com.amazon.deequ.comparison.{DataSynchronization, DatasetMatchFailed}
import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{DatasetMatchExecutableRule, Failed, Passed, RuleOutcome}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import software.amazon.glue.dqdl.model.DQRule

import scala.util.{Failure, Success, Try}

object DatasetMatchExecutor extends DQDLExecutor.RuleExecutor[DatasetMatchExecutableRule] {

  override def executeRules(rules: Seq[DatasetMatchExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame]): Map[DQRule, RuleOutcome] = {
    rules.map { rule =>
      rule.dqRule -> (additionalDataSources.get(rule.referenceDatasetAlias) match {
        case Some(referenceDF) =>
          val matchCols = rule.matchColumnMappings.orElse {
            Some(df.columns.filterNot(rule.keyColumnMappings.contains).map(c => c -> c).toMap)
          }
          val outcomeCol = java.util.UUID.randomUUID().toString

          Try(DataSynchronization.columnMatchRowLevel(
            df, referenceDF, rule.keyColumnMappings, matchCols, Some(outcomeCol))) match {
            case Success(Left(DatasetMatchFailed(errorMessage, _, _))) =>
              RuleOutcome(rule.dqRule, Failed, Some(errorMessage))
            case Success(Right(augmentedDF)) =>
              val filteredCount = augmentedDF.filter(col(outcomeCol) === lit(true)).count()
              val totalCount = df.count()
              if (totalCount == 0) {
                RuleOutcome(rule.dqRule, Failed, Some("Cannot evaluate: primary DataFrame is empty"))
              } else {
                val ratio = filteredCount.toDouble / totalCount
                val metrics = Map(s"Dataset.${rule.referenceDatasetAlias}.DatasetMatch" -> ratio)
                if (rule.assertion(ratio)) RuleOutcome(rule.dqRule, Passed, None, metrics)
                else RuleOutcome(rule.dqRule, Failed,
                  Some(s"Value: $ratio does not meet the constraint requirement."), metrics)
              }
            case Failure(ex) =>
              RuleOutcome(rule.dqRule, Failed,
                Some(s"Exception thrown while evaluating rule: ${ex.getClass.getName}"))
          }
        case None =>
          RuleOutcome(rule.dqRule, Failed,
            Some(s"${rule.referenceDatasetAlias} not found in additional sources"))
      })
    }.toMap
  }
}
