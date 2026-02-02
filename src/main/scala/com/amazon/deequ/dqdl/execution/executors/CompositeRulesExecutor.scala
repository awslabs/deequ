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
import com.amazon.deequ.dqdl.model.{CompositeExecutableRule, ExecutableRule, RuleOutcome}
import com.amazon.deequ.dqdl.translation.RuleOutcomeTranslator
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.DQRule

/**
 * Executor for composite rules that combine multiple rules with AND/OR operators.
 *
 * This executor handles the evaluation of composite rules by:
 * 1. Flattening the rule tree to extract all leaf (non-composite) rules
 * 2. Executing all leaf rules once (avoiding duplicate execution)
 * 3. Composing the outcomes using the specified logical operators
 *
 * Example composite rules:
 * - `(RowCount > 0) and (IsComplete "column")`
 * - `(ColumnValues "col" > 10) or (IsUnique "col")`
 * - `((Rule1) and (Rule2)) or (Rule3)` (nested composition)
 *
 * Note: Currently only supports dataset-level evaluation.
 */
object CompositeRulesExecutor extends DQDLExecutor.RuleExecutor[CompositeExecutableRule] {

  /**
   * Executes composite rules by evaluating all nested rules and composing their outcomes.
   *
   * The execution strategy:
   * 1. Flattens all composite rules to extract unique leaf rules
   * 2. Executes each leaf rule once (shared across multiple composites if needed)
   * 3. Uses RuleOutcomeTranslator to compose outcomes with AND/OR logic
   *
   * @param rules The composite rules to execute
   * @param df The DataFrame to evaluate rules against
   * @param additionalDataSources Additional DataFrames for dataset comparison rules
   * @return Map of rules to their composed outcomes
   */
  override def executeRules(
    rules: Seq[CompositeExecutableRule],
    df: DataFrame,
    additionalDataSources: Map[String, DataFrame] = Map.empty
  ): Map[DQRule, RuleOutcome] = {

    // Flatten all nested rules to get leaf rules
    val allNestedRules = rules.flatMap(flattenRules).distinct

    // Execute all nested rules
    val nestedOutcomes = DQDLExecutor.executeRules(
      allNestedRules,
      df,
      additionalDataSources
    )

    // Compose outcomes for each composite rule
    rules.map { compositeRule =>
      val outcome = RuleOutcomeTranslator.collectOutcome(
        compositeRule.dqRule,
        nestedOutcomes
      )
      compositeRule.dqRule -> outcome
    }.toMap
  }

  /**
   * Recursively flattens composite rules to extract all leaf (non-composite) rules.
   *
   * This ensures that each leaf rule is executed only once, even if it appears
   * in multiple composite rules or at different nesting levels.
   *
   * @param rule The composite rule to flatten
   * @return Sequence of all leaf rules contained within the composite
   */
  private def flattenRules(rule: CompositeExecutableRule): Seq[ExecutableRule] = {
    rule.nestedRules.flatMap {
      case composite: CompositeExecutableRule => flattenRules(composite)
      case other => Seq(other)
    }
  }
}
