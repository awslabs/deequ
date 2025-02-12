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

import com.amazon.deequ.analyzers.State
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.dqdl.model.{DeequMetricMapping, DeequRule, RuleToExecute, UnsupportedRule}
import com.amazon.deequ.metrics.Metric
import org.apache.log4j.Logger
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.{NumberBasedCondition, OperandEvaluator}

trait DQDLRuleTranslator[S <: State[_], +M <: Metric[_]] {
  val log: Logger = Logger.getLogger(getClass.getName)

  def operandEvaluator: OperandEvaluator

  private[dqdl] def translateRule(rule: DQRule): RuleToExecute =
    translateToDeequRule(rule) match {
      case Right(d) => d
      case Left(m) => UnsupportedRule(rule, Some(m))
    }

  private[dqdl] def isWhereClausePresent(rule: DQRule): Boolean = {
    rule.getWhereClause != null
  }

  private[dqdl] def translateToDeequRule(rule: DQRule): Either[String, DeequRule] = {
    var mutableCheck: Check = Check(CheckLevel.Error, java.util.UUID.randomUUID.toString)
    val optionalCheck: Either[String, (Check, Seq[DeequMetricMapping])] = rule.getRuleType match {
      case "RowCount" =>
        val fn = assertionAsScala(rule, rule.getCondition.asInstanceOf[NumberBasedCondition])
        mutableCheck = if (isWhereClausePresent(rule)) {
          mutableCheck.hasSize(rc => fn(rc.toDouble))
            .where(rule.getWhereClause)
        } else mutableCheck.hasSize(rc => fn(rc.toDouble))
        Right(
          (mutableCheck,
            Seq(DeequMetricMapping("Dataset", "*", "RowCount", "Size", None, rule = rule))))
      case _ => Left("Rule type not recognized")
    }
    optionalCheck.map { case (check, deequMetrics) => DeequRule(rule, check, deequMetrics) }
  }

  private[dqdl] def assertionAsScala(dqRule: DQRule, e: NumberBasedCondition): Double => Boolean = {
    val evaluator = operandEvaluator
    (d: Double) => e.evaluate(d, dqRule, evaluator)
  }

}
