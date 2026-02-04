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

import com.amazon.deequ.analyzers.FilteredRowOutcome
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.date.DateBasedCondition
import software.amazon.glue.dqdl.model.condition.date.DateBasedConditionOperator
import software.amazon.glue.dqdl.model.condition.date.DateExpression
import software.amazon.glue.dqdl.model.condition.duration.Duration
import software.amazon.glue.dqdl.model.condition.duration.DurationUnit

import scala.jdk.CollectionConverters._

class ColumnValuesDateRuleSpec extends AnyWordSpec with Matchers {

  "ColumnValuesDateRule" should {

    // Basic operators with dynamic dates
    "convert GREATER_THAN with now() - duration" in {
      val parameters = Map("TargetColumn" -> "created_at")
      val duration = new Duration(3, DurationUnit.DAYS)
      val dateExpr = new DateExpression.CurrentDateExpression(
        DateExpression.DateExpressionOperator.MINUS, duration)
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("> (now() - 3 days)",
        DateBasedConditionOperator.GREATER_THAN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      executableRule.column shouldBe "created_at"
      executableRule.dateColumnExpression.toString should include("created_at")
      executableRule.dateColumnExpression.toString should include(">")
    }

    "convert GREATER_THAN_EQUAL_TO with now()" in {
      val parameters = Map("TargetColumn" -> "order_date")
      val dateExpr = new DateExpression.CurrentDate()
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition(">= now()",
        DateBasedConditionOperator.GREATER_THAN_EQUAL_TO, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      executableRule.dateColumnExpression.toString should include(">=")
    }

    "convert LESS_THAN with now() + duration" in {
      val parameters = Map("TargetColumn" -> "expiry_date")
      val duration = new Duration(30, DurationUnit.DAYS)
      val dateExpr = new DateExpression.CurrentDateExpression(
        DateExpression.DateExpressionOperator.PLUS, duration)
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("< (now() + 30 days)",
        DateBasedConditionOperator.LESS_THAN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      executableRule.dateColumnExpression.toString should include("<")
    }

    "convert LESS_THAN_EQUAL_TO with now()" in {
      val parameters = Map("TargetColumn" -> "order_date")
      val dateExpr = new DateExpression.CurrentDate()
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("<= now()",
        DateBasedConditionOperator.LESS_THAN_EQUAL_TO, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      executableRule.dateColumnExpression.toString should include("<=")
    }

    "convert EQUALS with static date" in {
      val parameters = Map("TargetColumn" -> "birth_date")
      val dateExpr = new DateExpression.StaticDate("2000-01-01")
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("= \"2000-01-01\"",
        DateBasedConditionOperator.EQUALS, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      executableRule.dateColumnExpression.toString should include("2000-01-01")
    }

    "convert NOT_EQUALS with static date" in {
      val parameters = Map("TargetColumn" -> "event_date")
      val dateExpr = new DateExpression.StaticDate("2024-12-25")
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("!= \"2024-12-25\"",
        DateBasedConditionOperator.NOT_EQUALS, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      executableRule.dateColumnExpression.toString should include("NOT")
    }

    // Range operators
    "convert BETWEEN with two date expressions" in {
      val parameters = Map("TargetColumn" -> "event_date")
      val duration1 = new Duration(7, DurationUnit.DAYS)
      val duration2 = new Duration(1, DurationUnit.DAYS)
      val dateExpr1 = new DateExpression.CurrentDateExpression(
        DateExpression.DateExpressionOperator.MINUS, duration1)
      val dateExpr2 = new DateExpression.CurrentDateExpression(
        DateExpression.DateExpressionOperator.MINUS, duration2)
      val operands: java.util.List[DateExpression] =
        List[DateExpression](dateExpr1, dateExpr2).asJava
      val condition = new DateBasedCondition("between (now() - 7 days) and (now() - 1 day)",
        DateBasedConditionOperator.BETWEEN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      executableRule.dateColumnExpression.toString should include("AND")
    }

    "convert NOT_BETWEEN with static dates" in {
      val parameters = Map("TargetColumn" -> "order_date")
      val dateExpr1 = new DateExpression.StaticDate("2024-01-01")
      val dateExpr2 = new DateExpression.StaticDate("2024-12-31")
      val operands: java.util.List[DateExpression] =
        List[DateExpression](dateExpr1, dateExpr2).asJava
      val condition = new DateBasedCondition("not between \"2024-01-01\" and \"2024-12-31\"",
        DateBasedConditionOperator.NOT_BETWEEN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      // NOT_BETWEEN is expressed as (col <= lower) OR (col >= upper)
      executableRule.dateColumnExpression.toString should include("OR")
    }

    // Set operators
    "convert IN with multiple dates" in {
      val parameters = Map("TargetColumn" -> "holiday")
      val dateExpr1 = new DateExpression.StaticDate("2024-01-01")
      val dateExpr2 = new DateExpression.StaticDate("2024-12-25")
      val operands: java.util.List[DateExpression] =
        List[DateExpression](dateExpr1, dateExpr2).asJava
      val condition = new DateBasedCondition("in [\"2024-01-01\", \"2024-12-25\"]",
        DateBasedConditionOperator.IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      executableRule.dateColumnExpression.toString should include("IN")
    }

    "convert NOT_IN with dates" in {
      val parameters = Map("TargetColumn" -> "blackout_date")
      val dateExpr = new DateExpression.StaticDate("2024-07-04")
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("not in [\"2024-07-04\"]",
        DateBasedConditionOperator.NOT_IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      val executableRule = result.toOption.get
      executableRule.dateColumnExpression.toString should include("NOT")
    }

    // Duration units
    "handle HOURS duration unit" in {
      val parameters = Map("TargetColumn" -> "timestamp")
      val duration = new Duration(24, DurationUnit.HOURS)
      val dateExpr = new DateExpression.CurrentDateExpression(
        DateExpression.DateExpressionOperator.MINUS, duration)
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("> (now() - 24 hours)",
        DateBasedConditionOperator.GREATER_THAN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
    }

    "handle MINUTES duration unit" in {
      val parameters = Map("TargetColumn" -> "timestamp")
      val duration = new Duration(60, DurationUnit.MINUTES)
      val dateExpr = new DateExpression.CurrentDateExpression(
        DateExpression.DateExpressionOperator.MINUS, duration)
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("> (now() - 60 minutes)",
        DateBasedConditionOperator.GREATER_THAN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
    }

    // FilteredRowOutcome variations
    "handle FilteredRowOutcome.NULL" in {
      val parameters = Map("TargetColumn" -> "date_col")
      val dateExpr = new DateExpression.CurrentDate()
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("<= now()",
        DateBasedConditionOperator.LESS_THAN_EQUAL_TO, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.NULL)

      result.isRight shouldBe true
      result.toOption.get.filteredRow shouldBe FilteredRowOutcome.NULL
    }



    // Error cases
    "return error when TargetColumn is missing" in {
      val parameters = Map[String, String]()
      val dateExpr = new DateExpression.CurrentDate()
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("<= now()",
        DateBasedConditionOperator.LESS_THAN_EQUAL_TO, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isLeft shouldBe true
      result.left.toOption.get should include("TargetColumn")
    }

    // Column name edge cases
    "handle column names with spaces" in {
      val parameters = Map("TargetColumn" -> "Order Date")
      val dateExpr = new DateExpression.CurrentDate()
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("<= now()",
        DateBasedConditionOperator.LESS_THAN_EQUAL_TO, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      executableRule(result).column shouldBe "Order Date"
    }

    "handle column names with dots (backticks)" in {
      val parameters = Map("TargetColumn" -> "`Some.Date`")
      val dateExpr = new DateExpression.CurrentDate()
      val operands: java.util.List[DateExpression] = List[DateExpression](dateExpr).asJava
      val condition = new DateBasedCondition("<= now()",
        DateBasedConditionOperator.LESS_THAN_EQUAL_TO, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesDateRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
    }
  }

  private def executableRule(result: Either[String, com.amazon.deequ.dqdl.model.ColumnValuesDateExecutableRule]) =
    result.toOption.get
}
