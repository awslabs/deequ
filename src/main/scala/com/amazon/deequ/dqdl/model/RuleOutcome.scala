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

package com.amazon.deequ.dqdl.model

import com.amazon.deequ.checks.{CheckResult, CheckStatus}
import software.amazon.glue.dqdl.model.{DQRule, DQRuleLogicalOperator}

/**
 * Represents row-level evaluation outcomes for data quality rules.
 * This is used to track which specific rows pass or fail a rule.
 *
 * Note: Row-level evaluation is not yet implemented in DQDL. This data model
 * is provided for future extensibility and compatibility with AwsGlueMlDataQualityETL.
 *
 * A row-level outcome can be either:
 * - NoColumn: No row-level results available (default for dataset-level rules)
 * - SingularColumn: A single column contains the row-level results
 * - CompositeOutcome: A composite of multiple row-level outcomes combined with an operator
 */
sealed trait RowLevelOutcome

/**
 * Indicates that a rule does not produce row-level results.
 * This is the default for most DQDL rules which only evaluate at the dataset level.
 */
case class NoColumn() extends RowLevelOutcome

/**
 * Indicates that a rule produces row-level results in a single column.
 *
 * @param columnName The name of the column containing row-level boolean results
 */
case class SingularColumn(columnName: String) extends RowLevelOutcome

/**
 * Represents the composition of multiple row-level outcomes using logical operators.
 * Used by composite rules to combine row-level results from nested rules.
 *
 * @param columnName The name of the column that will contain the composed results
 * @param components The row-level outcomes from nested rules to be combined
 * @param operator The logical operator (AND/OR) used to combine the components
 */
case class CompositeOutcome(columnName: String,
                            components: Seq[RowLevelOutcome],
                            operator: DQRuleLogicalOperator) extends RowLevelOutcome

sealed trait OutcomeStatus {
  def asString: String

  def &&(other: OutcomeStatus): OutcomeStatus = this match {
    case Passed if (other == Passed) => Passed
    case _ => Failed
  }

  def ||(other: OutcomeStatus): OutcomeStatus = this match {
    case Passed => Passed
    case _ if other == Passed => Passed
    case _ => Failed
  }
}


case object Passed extends OutcomeStatus {
  def asString: String = "Passed"
}

case object Failed extends OutcomeStatus {
  def asString: String = "Failed"
}

/**
 * Represents the outcome of evaluating a data quality rule.
 *
 * @param rule The DQDL rule that was evaluated
 * @param outcome The status of the evaluation (Passed or Failed)
 * @param failureReason Optional message explaining why the rule failed
 * @param evaluatedMetrics Map of metric names to their computed values
 * @param evaluatedRule Optional rule with evaluated metric values filled in
 * @param rowLevelOutcome Information about row-level results (currently unused in DQDL)
 */
case class RuleOutcome(rule: DQRule,
                       outcome: OutcomeStatus,
                       failureReason: Option[String],
                       evaluatedMetrics: Map[String, Double] = Map.empty,
                       evaluatedRule: Option[DQRule] = None,
                       rowLevelOutcome: RowLevelOutcome = NoColumn()) {

  /**
   * Returns a copy of this outcome with the specified row-level outcome.
   * Note: Row-level evaluation is not yet implemented in DQDL.
   */
  def withRowLevelOutcome(outcome: RowLevelOutcome): RuleOutcome =
    copy(rowLevelOutcome = outcome)

  /**
   * Returns a copy of this outcome with the specified evaluated rule.
   */
  def withEvaluatedRule(rule: DQRule): RuleOutcome =
    copy(evaluatedRule = Some(rule))
}

object RuleOutcome {
  def apply(r: DQRule, checkResult: CheckResult): RuleOutcome = {
    val messages: Seq[String] = checkResult.constraintResults.flatMap { constraintResult =>
      constraintResult.message.map { message =>
        val metricName = constraintResult.metric.map(_.name).getOrElse("")
        if (metricName.equals("Completeness") && r.getRuleType.equals("ColumnValues")) {
          "Value: NULL does not meet the constraint requirement!"
        } else message
      }
    }
    val optionalMessage = messages.size match {
      case 0 => None
      case _ => Option(messages.mkString(System.lineSeparator()))
    }
    val checkPassed = checkResult.status == CheckStatus.Success
    RuleOutcome(r, if (checkPassed) Passed else Failed, optionalMessage)
  }
}
