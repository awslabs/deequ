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

package com.amazon.deequ.dqdl.translation

import com.amazon.deequ.dqdl.model.{CompositeOutcome, Failed, RuleOutcome}
import software.amazon.glue.dqdl.model.{DQRule, DQRuleLogicalOperator}

import scala.jdk.CollectionConverters._

/**
 * Translates and composes outcomes for composite rules.
 *
 * This object handles the recursive evaluation and composition of nested rules
 * using logical operators (AND/OR). It combines the outcomes, failure messages,
 * and evaluated metrics from all nested rules into a single composite outcome.
 *
 * Ported from AwsGlueMlDataQualityETL to maintain compatibility.
 */
object RuleOutcomeTranslator {

  /**
   * Recursively collects and composes outcomes for a rule and its nested rules.
   *
   * For composite rules, this method:
   * 1. Recursively evaluates all nested rules
   * 2. Combines their outcomes using the specified logical operator (AND/OR)
   * 3. Merges failure messages and evaluated metrics
   * 4. Composes row-level outcomes (for future row-level evaluation support)
   *
   * @param rule The DQDL rule to collect outcomes for
   * @param outcomeMap Map of already-evaluated rules to their outcomes
   * @return The composed outcome for the rule
   */
  def collectOutcome(
    rule: DQRule,
    outcomeMap: Map[DQRule, RuleOutcome]
  ): RuleOutcome = {
    rule.getRuleType match {
      case "Composite" =>
        composeOutcomes(rule, outcomeMap)
      case _ =>
        outcomeMap.getOrElse(
          rule,
          RuleOutcome(rule, Failed, Some("Rule not executed"))
        )
    }
  }

  /**
   * Composes outcomes from nested rules using AND/OR logic.
   *
   * AND logic: All nested rules must pass for the composite to pass
   * OR logic: At least one nested rule must pass for the composite to pass
   *
   * @param rule The composite rule containing nested rules
   * @param outcomeMap Map of already-evaluated rules to their outcomes
   * @return The composed outcome combining all nested rule results
   */
  private def composeOutcomes(
    rule: DQRule,
    outcomeMap: Map[DQRule, RuleOutcome]
  ): RuleOutcome = {
    // Validate that nested rules exist
    if (rule.getNestedRules == null || rule.getNestedRules.isEmpty) {
      return RuleOutcome(
        rule,
        Failed,
        Some("Composite rule must have at least one nested rule")
      )
    }

    val results: Seq[RuleOutcome] =
      rule.getNestedRules.asScala.map(r => collectOutcome(r, outcomeMap)).toSeq

    // Generate UUID for composite column (for row-level evaluation)
    val newColumnName = java.util.UUID.randomUUID().toString
    val rowLevelOutcomes = CompositeOutcome(
      newColumnName,
      results.map(_.rowLevelOutcome),
      rule.getOperator
    )

    // Compose evaluated rules
    val evaluatedComposite = composeEvaluatedRules(rule, outcomeMap)

    // Reduce outcomes using AND/OR logic
    val outcome = results.reduce { (a, b) =>
      val reducedMessage: Option[String] =
        Seq(a.failureReason, b.failureReason).flatten match {
          case Nil => None
          case seq => Some(seq.mkString(System.lineSeparator))
        }

      val outcomeStatus = if (rule.getOperator == DQRuleLogicalOperator.AND) {
        a.outcome && b.outcome
      } else {
        a.outcome || b.outcome
      }

      RuleOutcome(
        rule,
        outcomeStatus,
        reducedMessage,
        a.evaluatedMetrics ++ b.evaluatedMetrics
      )
    }.copy(rowLevelOutcome = rowLevelOutcomes)

    if (evaluatedComposite.isDefined) {
      outcome.withEvaluatedRule(evaluatedComposite.get)
    } else {
      outcome
    }
  }

  /**
   * Recursively composes evaluated rules with their metric values filled in.
   *
   * This method reconstructs the rule tree with evaluated metric values,
   * which is useful for debugging and understanding which specific values
   * caused a rule to pass or fail.
   *
   * @param rule The rule to compose evaluated versions for
   * @param outcomes Map of rules to their outcomes (containing evaluated rules)
   * @return The rule with evaluated metric values, or None if any nested rule lacks evaluation
   */
  private def composeEvaluatedRules(
    rule: DQRule,
    outcomes: Map[DQRule, RuleOutcome]
  ): Option[DQRule] = {
    if (rule.getNestedRules.isEmpty) {
      outcomes.get(rule).flatMap(_.evaluatedRule)
    } else {
      val evaluatedNestedRules =
        rule.getNestedRules.asScala.map(composeEvaluatedRules(_, outcomes))

      // If any nested rule is missing its evaluated rule, return None
      if (evaluatedNestedRules.exists(_.isEmpty)) {
        None
      } else {
        Some(rule.withNestedRules(evaluatedNestedRules.map(_.get).asJava))
      }
    }
  }
}
