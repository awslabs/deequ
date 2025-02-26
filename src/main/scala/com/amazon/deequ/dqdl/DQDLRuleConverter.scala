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
import com.amazon.deequ.dqdl.model.{DeequMetricMapping, DeequRule}
import com.amazon.deequ.dqdl.util.DataQualityUtility.{addWhereClause, isWhereClausePresent}
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition


trait DQDLRuleConverter {
  def translate(rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])]

  def assertionAsScala(dqRule: DQRule, e: NumberBasedCondition): Double => Boolean = {
    val evaluator = SimpleOperandEvaluator
    (d: Double) => e.evaluate(d, dqRule, evaluator)
  }
}

case class RowCountRuleConverter() extends DQDLRuleConverter {
  override def translate(rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])] = {
    val fn = assertionAsScala(rule, rule.getCondition.asInstanceOf[NumberBasedCondition])
    val check = Check(CheckLevel.Error, java.util.UUID.randomUUID.toString).hasSize(rc => fn(rc.toDouble))
    Right(addWhereClause(rule, check), Seq(DeequMetricMapping("Dataset", "*", "RowCount", "Size", None, rule = rule)))
  }
}
