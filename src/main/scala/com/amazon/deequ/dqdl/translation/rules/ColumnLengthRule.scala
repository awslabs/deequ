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
import com.amazon.deequ.dqdl.util.DQDLUtility.requiresToBeQuoted
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition

import scala.collection.JavaConverters._

case class ColumnLengthRule() extends DQDLRuleConverter {
  override def convert(rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])] = {
    val col = rule.getParameters.asScala("TargetColumn")
    val condition = rule.getCondition.asInstanceOf[NumberBasedCondition]

    mkColumnLengthCheck(Check(CheckLevel.Error, java.util.UUID.randomUUID.toString), col, condition, rule) match {
      case Right(check) =>
        val metricMappings = Seq(
          DeequMetricMapping("Column", col, "MinimumLength", "MinLength", None, rule = rule),
          DeequMetricMapping("Column", col, "MaximumLength", "MaxLength", None, rule = rule),
          DeequMetricMapping("Column", col, "LengthCompliance", "Compliance", Some(check.description), rule = rule)
        )
        Right((check, metricMappings))
      case Left(error) => Left(error)
    }
  }

  private def mkColumnLengthCheck(check: Check, targetColumn: String, condition: NumberBasedCondition,
                                  rule: DQRule): Either[String, Check] = {
    import software.amazon.glue.dqdl.model.condition.number.NumberBasedConditionOperator._
    import software.amazon.glue.dqdl.model.condition.number.AtomicNumberOperand
    import scala.collection.JavaConverters._

    val rawOperands = condition.getOperands.asScala
    if (!rawOperands.forall(_.isInstanceOf[AtomicNumberOperand])) {
      return Left("ColumnLength rule only supports atomic operands in conditions.")
    }

    val operands = rawOperands.map(_.getOperand.toDouble)
    val transformedColForSparkSql = if (requiresToBeQuoted(targetColumn)) s"`$targetColumn`" else targetColumn

    def withMultipleConstraints(minAssertion: Double => Boolean, maxAssertion: Double => Boolean): Check = {
      if (rule.getWhereClause != null) {
        check
          .hasMinLength(targetColumn, minAssertion).where(rule.getWhereClause)
          .hasMaxLength(targetColumn, maxAssertion).where(rule.getWhereClause)
      } else {
        check
          .hasMinLength(targetColumn, minAssertion)
          .hasMaxLength(targetColumn, maxAssertion)
      }
    }

    condition.getOperator match {
      case BETWEEN =>
        Right(withMultipleConstraints(_ > operands.head, _ < operands.last))

      case GREATER_THAN_EQUAL_TO =>
        Right(addWhereClause(rule, check.hasMinLength(targetColumn, _ >= operands.head)))

      case GREATER_THAN =>
        Right(addWhereClause(rule, check.hasMinLength(targetColumn, _ > operands.head)))

      case LESS_THAN_EQUAL_TO =>
        Right(addWhereClause(rule, check.hasMaxLength(targetColumn, _ <= operands.head)))

      case LESS_THAN =>
        Right(addWhereClause(rule, check.hasMaxLength(targetColumn, _ < operands.head)))

      case EQUALS =>
        Right(withMultipleConstraints(_ == operands.head, _ == operands.head))

      case NOT_EQUALS =>
        Right(withMultipleConstraints(_ != operands.head, _ != operands.head))

      case IN =>
        val complianceCondition =
          if (operands.contains(0.0)) {
            s"$transformedColForSparkSql is NULL or " +
              s"length($transformedColForSparkSql) in (${operands.mkString(",")})"
          } else {
            s"$transformedColForSparkSql is not NULL and " +
              s"length($transformedColForSparkSql) in (${operands.mkString(",")})"
          }
        Right(addWhereClause(rule, check.satisfies(complianceCondition, check.description, _ == 1.0,
          columns = List(transformedColForSparkSql))))

      case NOT_IN =>
        val complianceCondition =
          if (operands.contains(0.0)) {
            s"$transformedColForSparkSql is not NULL or " +
              s"length($transformedColForSparkSql) not in (${operands.mkString(",")})"
          } else {
            s"$transformedColForSparkSql is NULL or " +
              s"length($transformedColForSparkSql) not in (${operands.mkString(",")})"
          }
        Right(addWhereClause(rule, check.satisfies(complianceCondition, check.description, _ == 1.0,
          columns = List(transformedColForSparkSql))))

      case NOT_BETWEEN =>
        val notBetweenSparkSql = s"(length($transformedColForSparkSql) <= ${operands.head}) or " +
          s"(length($transformedColForSparkSql) >= ${operands.last})"
        val complianceCondition =
          if (operands.contains(0.0)) s"$transformedColForSparkSql is not NULL or ($notBetweenSparkSql)"
          else s"$transformedColForSparkSql is NULL or ($notBetweenSparkSql)"
        Right(addWhereClause(rule, check.satisfies(complianceCondition, check.description, _ == 1.0,
          columns = List(transformedColForSparkSql))))

      case _ => Left("Unsupported operator for ColumnLength rule.")
    }
  }
}
