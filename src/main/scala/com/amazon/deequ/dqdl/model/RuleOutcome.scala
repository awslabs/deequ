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
import software.amazon.glue.dqdl.model.DQRule

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

case class RuleOutcome(rule: DQRule,
                       outcome: OutcomeStatus,
                       failureReason: Option[String],
                       evaluatedMetrics: Map[String, Double] = Map.empty,
                       evaluatedRule: Option[DQRule] = None) {
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
