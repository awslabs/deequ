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
import software.amazon.glue.dqdl.model.condition.number.NullNumericOperand
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition
import software.amazon.glue.dqdl.model.condition.number.NumberBasedConditionOperator._
import software.amazon.glue.dqdl.model.condition.string.KeywordStringOperand
import software.amazon.glue.dqdl.model.condition.string.QuotedStringOperand
import software.amazon.glue.dqdl.model.condition.string.StringBasedCondition
import software.amazon.glue.dqdl.model.condition.string.StringBasedConditionOperator

import scala.collection.JavaConverters._

case class ColumnValuesRule() extends DQDLRuleConverter {

  override def convert(rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])] = {
    val targetColumn = rule.getParameters.asScala("TargetColumn")
    val transformedCol = if (requiresToBeQuoted(targetColumn)) s"`$targetColumn`" else targetColumn
    val check = Check(CheckLevel.Error, java.util.UUID.randomUUID.toString)

    rule.getCondition match {
      case condition: NumberBasedCondition =>
        mkNumericCheck(check, targetColumn, transformedCol, condition, rule)
      case condition: StringBasedCondition =>
        mkStringCheck(check, targetColumn, transformedCol, condition, rule)
      case _ =>
        Left(s"Unsupported condition type for ColumnValues rule: " +
          s"${Option(rule.getCondition).map(_.getClass.getSimpleName).getOrElse("null")}")
    }
  }

  private def mkNumericCheck(check: Check, targetColumn: String, transformedCol: String,
                             condition: NumberBasedCondition,
                             rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])] = {
    val rawOperands = condition.getOperands.asScala
    if (!rawOperands.forall(op => op.isInstanceOf[AtomicNumberOperand] || op.isInstanceOf[NullNumericOperand])) {
      return Left("ColumnValues rule only supports atomic operands or NULL keyword in conditions.")
    }
    if (rawOperands.isEmpty) {
      return Left("ColumnValues rule requires at least one operand.")
    }

    val hasNullOperand = rawOperands.exists(_.isInstanceOf[NullNumericOperand])
    val numericOperands = rawOperands.collect { case a: AtomicNumberOperand => a.getOperand.toDouble }

    condition.getOperator match {
      case GREATER_THAN =>
        val resultCheck = if (isWhereClausePresent(rule)) {
          check
            .hasMin(targetColumn, _ > numericOperands.head).where(rule.getWhereClause)
            .isComplete(targetColumn).where(rule.getWhereClause)
        } else {
          check
            .hasMin(targetColumn, _ > numericOperands.head)
            .isComplete(targetColumn)
        }
        Right((resultCheck, minMetric(targetColumn, rule)))

      case GREATER_THAN_EQUAL_TO =>
        val resultCheck = if (isWhereClausePresent(rule)) {
          check
            .hasMin(targetColumn, _ >= numericOperands.head).where(rule.getWhereClause)
            .isComplete(targetColumn).where(rule.getWhereClause)
        } else {
          check
            .hasMin(targetColumn, _ >= numericOperands.head)
            .isComplete(targetColumn)
        }
        Right((resultCheck, minMetric(targetColumn, rule)))

      case LESS_THAN =>
        val resultCheck = if (isWhereClausePresent(rule)) {
          check
            .hasMax(targetColumn, _ < numericOperands.head).where(rule.getWhereClause)
            .isComplete(targetColumn).where(rule.getWhereClause)
        } else {
          check
            .hasMax(targetColumn, _ < numericOperands.head)
            .isComplete(targetColumn)
        }
        Right((resultCheck, maxMetric(targetColumn, rule)))

      case LESS_THAN_EQUAL_TO =>
        val resultCheck = if (isWhereClausePresent(rule)) {
          check
            .hasMax(targetColumn, _ <= numericOperands.head).where(rule.getWhereClause)
            .isComplete(targetColumn).where(rule.getWhereClause)
        } else {
          check
            .hasMax(targetColumn, _ <= numericOperands.head)
            .isComplete(targetColumn)
        }
        Right((resultCheck, maxMetric(targetColumn, rule)))

      case BETWEEN =>
        if (numericOperands.size < 2) {
          return Left("BETWEEN requires two operands.")
        }
        val resultCheck = if (isWhereClausePresent(rule)) {
          check.isContainedIn(targetColumn, numericOperands.head, numericOperands.last,
            includeLowerBound = false, includeUpperBound = false).where(rule.getWhereClause)
            .isComplete(targetColumn).where(rule.getWhereClause)
        } else {
          check.isContainedIn(targetColumn, numericOperands.head, numericOperands.last,
            includeLowerBound = false, includeUpperBound = false)
            .isComplete(targetColumn)
        }
        Right((resultCheck, complianceMetric(targetColumn, check.description, rule)))

      case NOT_BETWEEN =>
        if (numericOperands.size < 2) {
          return Left("NOT BETWEEN requires two operands.")
        }
        val sql = s"$transformedCol IS NULL OR " +
          s"($transformedCol <= ${numericOperands.head} OR $transformedCol >= ${numericOperands.last})"
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(targetColumn, check.description, rule)))

      case IN =>
        val nums = numericOperands.mkString(", ")
        val sql = (numericOperands.nonEmpty, hasNullOperand) match {
          case (true, false) => s"$transformedCol IS NOT NULL AND $transformedCol IN ($nums)"
          case (true, true) => s"$transformedCol IN ($nums) OR $transformedCol IS NULL"
          case (false, true) => s"$transformedCol IS NULL"
          case _ => "FALSE"
        }
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(targetColumn, check.description, rule)))

      case NOT_IN =>
        val nums = numericOperands.mkString(", ")
        val sql = (numericOperands.nonEmpty, hasNullOperand) match {
          case (true, false) => s"$transformedCol IS NULL OR $transformedCol NOT IN ($nums)"
          case (true, true) => s"$transformedCol NOT IN ($nums) AND $transformedCol IS NOT NULL"
          case (false, true) => s"$transformedCol IS NOT NULL"
          case _ => "TRUE"
        }
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(targetColumn, check.description, rule)))

      case EQUALS =>
        if (hasNullOperand) {
          val sql = s"$transformedCol IS NULL"
          Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
            columns = List(transformedCol))), complianceMetric(targetColumn, check.description, rule)))
        } else {
          val resultCheck = if (isWhereClausePresent(rule)) {
            check
              .hasMin(targetColumn, _ == numericOperands.head).where(rule.getWhereClause)
              .hasMax(targetColumn, _ == numericOperands.head).where(rule.getWhereClause)
              .isComplete(targetColumn).where(rule.getWhereClause)
          } else {
            check
              .hasMin(targetColumn, _ == numericOperands.head)
              .hasMax(targetColumn, _ == numericOperands.head)
              .isComplete(targetColumn)
          }
          Right((resultCheck, minMetric(targetColumn, rule) ++ maxMetric(targetColumn, rule)))
        }

      case NOT_EQUALS =>
        if (hasNullOperand) {
          val sql = s"$transformedCol IS NOT NULL"
          Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
            columns = List(transformedCol))), complianceMetric(targetColumn, check.description, rule)))
        } else {
          val sql = s"$transformedCol IS NULL OR $transformedCol != ${numericOperands.head}"
          Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
            columns = List(transformedCol))), complianceMetric(targetColumn, check.description, rule)))
        }

      case _ =>
        Left(s"Unsupported operator for ColumnValues numeric condition: ${condition.getOperator}")
    }
  }

  private def mkStringCheck(check: Check, targetColumn: String, transformedCol: String,
                            condition: StringBasedCondition,
                            rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])] = {
    condition.getOperator match {
      case StringBasedConditionOperator.MATCHES =>
        val pattern = extractPattern(condition)
        val fullRegex = s"^${pattern}$$".r
        Right((addWhereClause(rule, check.hasPattern(targetColumn, fullRegex)),
          Seq(DeequMetricMapping("Column", targetColumn, "PatternMatch", "PatternMatch", None, rule = rule))))

      case StringBasedConditionOperator.NOT_MATCHES =>
        val pattern = extractPattern(condition)
        val fullRegex = s"^(?!\\b${pattern}\\b).*$$".r
        Right((addWhereClause(rule, check.hasPattern(targetColumn, fullRegex)),
          Seq(DeequMetricMapping("Column", targetColumn, "PatternMatch", "PatternMatch", None, rule = rule))))

      case StringBasedConditionOperator.IN | StringBasedConditionOperator.EQUALS =>
        val sql = constructComplianceCondition(transformedCol, condition, isNegated = false)
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(targetColumn, check.description, rule)))

      case StringBasedConditionOperator.NOT_IN | StringBasedConditionOperator.NOT_EQUALS =>
        val sql = constructComplianceCondition(transformedCol, condition, isNegated = true)
        Right((addWhereClause(rule, check.satisfies(sql, check.description, _ == 1.0,
          columns = List(transformedCol))), complianceMetric(targetColumn, check.description, rule)))

      case _ =>
        Left(s"Unsupported operator for ColumnValues string condition: ${condition.getOperator}")
    }
  }

  private def constructComplianceCondition(targetColumn: String, condition: StringBasedCondition,
                                           isNegated: Boolean): String = {
    val operands = condition.getOperands.asScala
    val quotedStrings = operands.collect { case q: QuotedStringOperand => q.getOperand }
    val keywordOperands = operands.collect { case k: KeywordStringOperand => k.formatOperand() }

    val hasNull = keywordOperands.contains("NULL")
    val hasEmpty = keywordOperands.contains("EMPTY")
    val hasWhitespacesOnly = keywordOperands.contains("WHITESPACES_ONLY")

    val conditions = scala.collection.mutable.ListBuffer[String]()

    if (isNegated) {
      if (hasNull) conditions += s"$targetColumn IS NOT NULL"
      if (hasEmpty) conditions += s"$targetColumn != ''"
      if (hasWhitespacesOnly) {
        conditions += s"(LENGTH(TRIM($targetColumn)) > 0 OR LENGTH($targetColumn) = 0)"
      }
      if (quotedStrings.nonEmpty) {
        val valueList = quotedStrings.map(s => s"'${s.replace("'", "''")}'").mkString(", ")
        conditions += s"($targetColumn IS NULL OR $targetColumn NOT IN ($valueList))"
      }
      if (conditions.isEmpty) "TRUE" else conditions.mkString(" AND ")
    } else {
      if (hasNull) conditions += s"$targetColumn IS NULL"
      if (hasEmpty) conditions += s"$targetColumn = ''"
      if (hasWhitespacesOnly) {
        conditions += s"(LENGTH(TRIM($targetColumn)) = 0 AND LENGTH($targetColumn) > 0)"
      }
      if (quotedStrings.nonEmpty) {
        val valueList = quotedStrings.map(s => s"'${s.replace("'", "''")}'").mkString(", ")
        conditions += s"($targetColumn IS NOT NULL AND $targetColumn IN ($valueList))"
      }
      if (conditions.isEmpty) "FALSE" else conditions.mkString(" OR ")
    }
  }

  private def minMetric(targetColumn: String, rule: DQRule): Seq[DeequMetricMapping] =
    Seq(DeequMetricMapping("Column", targetColumn, "Minimum", "Minimum", None, rule = rule))

  private def maxMetric(targetColumn: String, rule: DQRule): Seq[DeequMetricMapping] =
    Seq(DeequMetricMapping("Column", targetColumn, "Maximum", "Maximum", None, rule = rule))

  private def complianceMetric(targetColumn: String, desc: String, rule: DQRule): Seq[DeequMetricMapping] =
    Seq(DeequMetricMapping("Column", targetColumn, "ColumnValues.Compliance", "Compliance", Some(desc),
      rule = rule))

  private def extractPattern(condition: StringBasedCondition): String =
    condition.getOperands.asScala.head match {
      case q: QuotedStringOperand => q.getOperand
      case other => other.toString
    }
}
