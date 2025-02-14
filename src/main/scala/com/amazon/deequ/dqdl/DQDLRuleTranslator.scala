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
import com.amazon.deequ.dqdl.model._
import com.amazon.deequ.metrics.Metric
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import software.amazon.glue.dqdl.model.condition.number.{NumberBasedCondition, OperandEvaluator}
import software.amazon.glue.dqdl.model.{DQRule, DQRuleset}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.asScalaBufferConverter

case class FlattenedRuleset[S <: State[_], +M <: Metric[_]](deequRules: Seq[DeequRule],
                                                            unsupportedRules: Seq[UnsupportedRule])


trait DQDLRuleTranslator[S <: State[_], +M <: Metric[_]] {

  def operandEvaluator: OperandEvaluator

  val log = Logger.getLogger(getClass.getName)

  private def isWhereClausePresent(rule: DQRule): Boolean = {
    rule.getWhereClause != null
  }

  def flattenRuleset(ruleset: DQRuleset, schema: StructType): FlattenedRuleset[S, M] = {
    val flattenedDQRules: Seq[DQRule] = flattenDQRule(ruleset).distinct
    val translatedRules: Seq[RuleToExecute] = flattenedDQRules.map(translateRule)
    val (deequRules, unsupportedRules) = splitRules(translatedRules)

    FlattenedRuleset[S, M](deequRules, unsupportedRules)
  }

  private[dqdl] def splitRules(rules: Seq[RuleToExecute]): (Seq[DeequRule], Seq[UnsupportedRule]) = {
    @tailrec
    def splitRules(rules: Seq[RuleToExecute],
                   deequRulesAcc: Seq[DeequRule],

                   unsupportedRulesAcc: Seq[UnsupportedRule]): (Seq[DeequRule], Seq[UnsupportedRule]) = {
      rules match {
        case head +: tail => head match {

          case d: DeequRule =>
            splitRules(tail, deequRulesAcc :+ d, unsupportedRulesAcc)
          case u: UnsupportedRule =>
            splitRules(tail, deequRulesAcc, unsupportedRulesAcc :+ u)
        }
        case Nil => (deequRulesAcc, unsupportedRulesAcc)
      }
    }

    splitRules(rules, Seq.empty, Seq.empty)
  }

  private[dqdl] def translateRule(rule: DQRule): RuleToExecute =
    translateToDeequRule(rule) match {
      case Right(d) => d
      case Left(m) => UnsupportedRule(rule, Some(m))
    }

  private[dqdl] def flattenDQRule(ruleset: DQRuleset): Seq[DQRule] = {
    def flattenDQRuleRecursive(rule: DQRule, flattenedRules: Seq[DQRule]): Seq[DQRule] = rule.getRuleType match {
      case "Composite" => rule.getNestedRules.asScala.flatMap(r => flattenDQRuleRecursive(r, Seq.empty))
      case _ => flattenedRules :+ rule
    }

    ruleset.getRules.asScala.flatMap(r => flattenDQRuleRecursive(r, Seq.empty))
  }

  private[dqdl] def assertionAsScala(dqRule: DQRule, e: NumberBasedCondition): Double => Boolean = {
    val evaluator = operandEvaluator
    (d: Double) => e.evaluate(d, dqRule, evaluator)
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
      case _ => Left("Unrecognized condition")
    }
    optionalCheck.map { case (check, deequMetrics) => DeequRule(rule, check, deequMetrics) }
  }

}
