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

package com.amazon.deequ.dqdl.translation.rules

import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.dqdl.model.DeequMetricMapping
import com.amazon.deequ.dqdl.translation.DQDLRuleConverter
import com.amazon.deequ.dqdl.util.DQDLUtility.addWhereClause
import com.amazon.deequ.dqdl.util.DQDLUtility.isWhereClausePresent
import com.amazon.deequ.dqdl.util.DQDLUtility.requiresToBeQuoted
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.AtomicNumberOperand
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition
import software.amazon.glue.dqdl.model.condition.number.NumberBasedConditionOperator._
import software.amazon.glue.dqdl.model.condition.string.KeywordStringOperand
import software.amazon.glue.dqdl.model.condition.string.QuotedStringOperand
import software.amazon.glue.dqdl.model.condition.string.StringBasedCondition
import software.amazon.glue.dqdl.model.condition.string.StringBasedConditionOperator

import scala.collection.JavaConverters._

case class ColumnValuesRule() extends DQDLRuleConverter {
  override def convert(rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])] = {
    val col = rule.getParameters.asScala("TargetColumn")
    val check = Check(CheckLevel.Error, java.util.UUID.randomUUID.toString)

    rule.getCondition match {
      case condition: NumberBasedCondition =>
        mkNumericCheck(check, col, condition, rule)
      case condition: StringBasedCondition =>
        mkStringCheck(check, col, condition, rule)
      case _ =>
        Left("Unsupported condition type for ColumnValues rule.")
    }
  }

  private def mkNumericCheck(check: Check, col: String, condition: NumberBasedCondition,
                             rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])] = {
    val rawOperands = condition.getOperands.asScala
    if (!rawOperands.forall(_.isInstanceOf[AtomicNumberOperand])) {
      return Left("ColumnValues rule only supports atomic operands in numeric conditions.")
    }

    val operands = rawOperands.map(_.getOperand.toDouble)
    val transformedCol = if (requiresToBeQuoted(col)) s"`$col`" else col

    condition.getOperator match {
      case GREATER_THAN =>
        Right((addWhereClause(rule, check.hasMin(col, _ > operands.head)), minMetric(col, rule)))

      case GREATER_THAN_EQUAL_TO =>
        Right((addWhereClause(rule, check.hasMin(col, _ >= operands.head)), minMetric(col, rule)))

      case LESS_THAN =>
        Right((addWhereClause(rule, check.hasMax(col, _ < operands.head)), maxMetric(col, rule)))

      case LESS_THAN_EQUAL_TO =>
        Right((addWhereClause(rule, check.hasMax(col, _ <= operands.head)), maxMetric(col, rule)))

      case BETWEEN =>
        val resultCheck = if (isWhereClausePresent(rule)) {
          check.hasMin(col, _ >= operands.head).where(rule.getWhereClause)
            .hasMax(col, _ <= operands.last).where(rule.getWhereClause)
        } else {
          check.hasMin(col, _ >= operands.head).hasMax(col, _ <= operands.last)
        }
        Right((resultCheck, minMetric(col, rule) ++ maxMetric(col, rule)))

      case NOT_BETWEEN =>
        val sql = s"$transformedCol IS NULL OR " +
          s"$transformedCol < ${operands.head} OR $transformedCol > ${operands.last}"
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(col, check.description, rule)))

      case IN =>
        val sql = s"$transformedCol IS NULL OR $transformedCol IN (${operands.mkString(",")})"
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(col, check.description, rule)))

      case NOT_IN =>
        val sql = s"$transformedCol IS NULL OR $transformedCol NOT IN (${operands.mkString(",")})"
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(col, check.description, rule)))

      case EQUALS =>
        val sql = s"$transformedCol IS NULL OR $transformedCol = ${operands.head}"
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(col, check.description, rule)))

      case NOT_EQUALS =>
        val sql = s"$transformedCol IS NULL OR $transformedCol != ${operands.head}"
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(col, check.description, rule)))

      case _ => Left("Unsupported operator for ColumnValues numeric condition.")
    }
  }

  private def mkStringCheck(check: Check, col: String, condition: StringBasedCondition,
                            rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])] = {
    val transformedCol = if (requiresToBeQuoted(col)) s"`$col`" else col

    condition.getOperator match {
      case StringBasedConditionOperator.MATCHES =>
        val pattern = condition.getOperands.asScala.head match {
          case q: QuotedStringOperand => q.getOperand
          case other => other.toString
        }
        Right((addWhereClause(rule, check.hasPattern(col, pattern.r)),
          Seq(DeequMetricMapping("Column", col, "PatternMatch", "PatternMatch", None, rule = rule))))

      case StringBasedConditionOperator.NOT_MATCHES =>
        val pattern = condition.getOperands.asScala.head match {
          case q: QuotedStringOperand => q.getOperand
          case other => other.toString
        }
        val sql = s"$transformedCol IS NULL OR NOT $transformedCol RLIKE '${pattern.replace("'", "\\'")}'"
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(col, check.description, rule)))

      case StringBasedConditionOperator.IN =>
        val sql = constructComplianceCondition(transformedCol, condition, isNegated = false)
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(col, check.description, rule)))

      case StringBasedConditionOperator.NOT_IN =>
        val sql = constructComplianceCondition(transformedCol, condition, isNegated = true)
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(col, check.description, rule)))

      case _ => Left("Unsupported operator for ColumnValues string condition.")
    }
  }

  private def constructComplianceCondition(col: String, condition: StringBasedCondition,
                                           isNegated: Boolean): String = {
    val operands = condition.getOperands.asScala
    val quotedStrings = operands.collect { case q: QuotedStringOperand => q.getOperand }
    val keywordOperands = operands.collect { case k: KeywordStringOperand => k }

    val hasNull = keywordOperands.exists(_.formatOperand() == "NULL")
    val hasEmpty = keywordOperands.exists(_.formatOperand() == "EMPTY")
    val hasWhitespacesOnly = keywordOperands.exists(_.formatOperand() == "WHITESPACES_ONLY")

    val conditions = scala.collection.mutable.ListBuffer[String]()

    if (isNegated) {
      if (hasNull) conditions += s"$col IS NOT NULL"
      if (hasEmpty) conditions += s"$col != ''"
      if (hasWhitespacesOnly) conditions += s"TRIM($col) != ''"
      if (quotedStrings.nonEmpty) {
        val valueList = quotedStrings.map(s => s"'${s.replace("'", "\\'")}'").mkString(",")
        conditions += s"$col NOT IN ($valueList)"
      }
      if (conditions.isEmpty) "TRUE" else conditions.mkString(" AND ")
    } else {
      if (hasNull) conditions += s"$col IS NULL"
      if (hasEmpty) conditions += s"$col = ''"
      if (hasWhitespacesOnly) conditions += s"TRIM($col) = ''"
      if (quotedStrings.nonEmpty) {
        val valueList = quotedStrings.map(s => s"'${s.replace("'", "\\'")}'").mkString(",")
        conditions += s"$col IN ($valueList)"
      }
      if (conditions.isEmpty) "FALSE" else conditions.mkString(" OR ")
    }
  }

  private def minMetric(col: String, rule: DQRule): Seq[DeequMetricMapping] =
    Seq(DeequMetricMapping("Column", col, "Minimum", "Minimum", None, rule = rule))

  private def maxMetric(col: String, rule: DQRule): Seq[DeequMetricMapping] =
    Seq(DeequMetricMapping("Column", col, "Maximum", "Maximum", None, rule = rule))

  private def complianceMetric(col: String, desc: String, rule: DQRule): Seq[DeequMetricMapping] =
    Seq(DeequMetricMapping("Column", col, "ColumnValues.Compliance", "Compliance", Some(desc),
      rule = rule))
}
