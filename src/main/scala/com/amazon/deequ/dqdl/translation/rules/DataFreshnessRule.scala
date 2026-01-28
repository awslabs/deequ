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
import com.amazon.deequ.dqdl.model.DataFreshnessExecutableRule
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, current_timestamp, round, to_timestamp}
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.duration.{Duration, DurationBasedCondition, DurationBasedConditionOperator, DurationUnit}

import scala.jdk.CollectionConverters._

object DataFreshnessRule {

  def toExecutableRule(rule: DQRule, filteredRowOutcome: FilteredRowOutcome.FilteredRowOutcome):
    Either[String, DataFreshnessExecutableRule] = {
    val columnName = rule.getParameters.asScala("TargetColumn").replaceAll("\"", "")

    rule.getCondition match {
      case condition: DurationBasedCondition =>
        val expr = durationConditionToFreshnessColumnExpression(columnName, condition)
        Right(DataFreshnessExecutableRule(rule, columnName, expr, filteredRowOutcome))
      case _ =>
        Left("Unsupported condition type for DataFreshness rule")
    }
  }

  private def durationConditionToFreshnessColumnExpression(
    targetColumn: String,
    durationCondition: DurationBasedCondition
  ): Column = {
    val targetTs = to_timestamp(col(targetColumn)).cast("long").as("target_ts")
    val currentTs = to_timestamp(current_timestamp()).cast("long").as("current_ts")
    val diffSecs = currentTs - targetTs
    val diffHours = round(diffSecs / 3600, 2)

    val evaluatedOperands = durationCondition.getOperands.asScala.map(evaluateDurationToHours)

    durationCondition.getOperator match {
      case DurationBasedConditionOperator.BETWEEN =>
        (diffHours > evaluatedOperands.head) && (diffHours < evaluatedOperands.last)
      case DurationBasedConditionOperator.NOT_BETWEEN =>
        (diffHours <= evaluatedOperands.head) || (diffHours >= evaluatedOperands.last)
      case DurationBasedConditionOperator.GREATER_THAN_EQUAL_TO =>
        diffHours >= evaluatedOperands.head
      case DurationBasedConditionOperator.GREATER_THAN =>
        diffHours > evaluatedOperands.head
      case DurationBasedConditionOperator.LESS_THAN_EQUAL_TO =>
        diffHours <= evaluatedOperands.head
      case DurationBasedConditionOperator.LESS_THAN =>
        diffHours < evaluatedOperands.head
      case DurationBasedConditionOperator.EQUALS =>
        diffHours === evaluatedOperands.head
      case DurationBasedConditionOperator.NOT_EQUALS =>
        diffHours =!= evaluatedOperands.head
      case DurationBasedConditionOperator.IN =>
        diffHours.isin(evaluatedOperands: _*)
      case DurationBasedConditionOperator.NOT_IN =>
        !diffHours.isin(evaluatedOperands: _*)
    }
  }

  private def evaluateDurationToHours(duration: Duration): Double = {
    duration.getUnit match {
      case DurationUnit.DAYS =>
        24 * duration.getAmount.toDouble
      case DurationUnit.HOURS =>
        duration.getAmount.toDouble
      case DurationUnit.MINUTES =>
        if (duration.getAmount == 0) 0.0
        else new java.math.BigDecimal(duration.getAmount / 60.0)
          .setScale(4, java.math.RoundingMode.HALF_UP)
          .doubleValue
    }
  }
}
