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

package com.amazon.deequ.dqdl.execution

import com.amazon.deequ.dqdl.execution.executors.{AggregateMatchExecutor, ColumnNamesMatchPatternExecutor, CompositeRulesExecutor, DataFreshnessExecutor, DatasetMatchExecutor, DeequRulesExecutor, ReferentialIntegrityExecutor, RowCountMatchExecutor, UnsupportedRulesExecutor}
import com.amazon.deequ.dqdl.model.{AggregateMatchExecutableRule, ColumnNamesMatchPatternExecutableRule, CompositeExecutableRule, DataFreshnessExecutableRule, DatasetMatchExecutableRule, DeequExecutableRule, ExecutableRule, Failed, ReferentialIntegrityExecutableRule, RowCountMatchExecutableRule, RuleOutcome, UnsupportedExecutableRule}
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.DQRule


/**
 * Executes DQDL rules on a Spark DataFrame.
 */
object DQDLExecutor {

  trait RuleExecutor[T <: ExecutableRule] {
    def executeRules(rules: Seq[T], df: DataFrame,
                     additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome]
  }

  // Map from rule class to its executor
  private val executors = Map[Class[_ <: ExecutableRule], RuleExecutor[_ <: ExecutableRule]](
    classOf[DeequExecutableRule] -> DeequRulesExecutor,
    classOf[CompositeExecutableRule] -> CompositeRulesExecutor,
    classOf[UnsupportedExecutableRule] -> UnsupportedRulesExecutor,
    classOf[RowCountMatchExecutableRule] -> RowCountMatchExecutor,
    classOf[ReferentialIntegrityExecutableRule] -> ReferentialIntegrityExecutor,
    classOf[DataFreshnessExecutableRule] -> DataFreshnessExecutor,
    classOf[ColumnNamesMatchPatternExecutableRule] -> ColumnNamesMatchPatternExecutor,
    classOf[DatasetMatchExecutableRule] -> DatasetMatchExecutor,
    classOf[AggregateMatchExecutableRule] -> AggregateMatchExecutor
  )

  def executeRules(rules: Seq[ExecutableRule], df: DataFrame,
                   additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    // Group rules to execute each group with the corresponding executor
    val rulesByType = rules.groupBy(_.getClass)

    rulesByType.flatMap {
      case (ruleClass, rules) =>
        executors.get(ruleClass) match {
          case Some(executor) =>
            executor.asInstanceOf[RuleExecutor[ExecutableRule]].executeRules(rules, df, additionalDataSources)
          case None => handleError(rules)
        }
    }
  }

  private def handleError(rules: Seq[ExecutableRule]) = {
    rules.map { rule =>
      rule.dqRule -> RuleOutcome(
        rule.dqRule,
        Failed,
        Some(s"No executor found for rule type: ${rule.dqRule.getRuleType}")
      )
    }

  }
}
