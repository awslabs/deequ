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

package com.amazon.deequ.dqdl

import com.amazon.deequ.checks.{Check, CheckLevel}
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition


trait DQDLRuleConverter {
  def translate(rule: DQRule): Option[Check]

  // todo move
  def isWhereClausePresent(rule: DQRule): Boolean = {
    rule.getWhereClause != null
  }

  def assertionAsScala(dqRule: DQRule, e: NumberBasedCondition): Double => Boolean = {
    val evaluator = SimpleOperandEvaluator
    (d: Double) => e.evaluate(d, dqRule, evaluator)
  }
}

case class CompletenessRuleConverter() extends DQDLRuleConverter {
  override def translate(rule: DQRule): Option[Check] = {
    // todo implement
    Some(Check(CheckLevel.Error, s"Completeness check: ${rule.getRuleType}"))
  }
}

case class RowCountRuleConverter() extends DQDLRuleConverter {
  override def translate(rule: DQRule): Option[Check] = {
    var mutableCheck: Check = Check(CheckLevel.Error, java.util.UUID.randomUUID.toString)
    val fn = assertionAsScala(rule, rule.getCondition.asInstanceOf[NumberBasedCondition])
    mutableCheck = if (isWhereClausePresent(rule)) {
      mutableCheck.hasSize(rc => fn(rc.toDouble)).where(rule.getWhereClause)
    } else mutableCheck.hasSize(rc => fn(rc.toDouble))
    Some(mutableCheck)
  }
}
