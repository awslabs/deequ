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

package com.amazon.deequ.dqdl.util

import com.amazon.deequ.analyzers.FilteredRowOutcome
import com.amazon.deequ.analyzers.FilteredRowOutcome.FilteredRowOutcome
import com.amazon.deequ.dqdl.model.{RuleOutcome, Passed, Failed}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import software.amazon.glue.dqdl.model.DQRule
import scala.util.{Try, Success, Failure}

object RuleEvaluationHelper {

  def evaluateRuleAgainstColumn(
    df: DataFrame,
    rule: DQRule,
    targetColumn: String,
    filteredRowOutcome: FilteredRowOutcome,
    metricName: String,
    outcomeExpression: Column,
    assertion: Option[Double => Boolean] = None
  ): RuleOutcome = {

    val whereClause = Option(rule.getWhereClause)

    // Check if target column exists
    if (Try(df(targetColumn)).isFailure) {
      return RuleOutcome(
        rule,
        Failed,
        Some(s"Column $targetColumn does not exist in the dataset")
      )
    }

    // Validate where clause if provided
    val validatedFilteredDf = whereClause match {
      case Some(where) =>
        Try(df.where(where)).toOption match {
          case Some(filtered) => Some(filtered)
          case None =>
            return RuleOutcome(
              rule,
              Failed,
              Some("The provided where clause is invalid")
            )
        }
      case None => None
    }

    Try {
      val filteredDf = validatedFilteredDf.getOrElse(df)
      val totalCount = filteredDf.count()

      if (totalCount == 0) {
        RuleOutcome(rule, Passed, Some("No rows matched the filter"))
      } else {
        val tempCol = java.util.UUID.randomUUID().toString
        val augmentedDfWithResults = whereClause match {
          case Some(where) =>
            df.withColumn(tempCol, when(not(expr(where)), null).otherwise(outcomeExpression))
          case None =>
            df.withColumn(tempCol, outcomeExpression)
        }

        val filteredCount = augmentedDfWithResults.where(col(tempCol) === true).count()
        val ratio = filteredCount.toDouble / totalCount

        val evaluatedMetrics = Map(metricName -> ratio)

        assertion match {
          case Some(assertFn) =>
            if (assertFn(ratio)) {
              RuleOutcome(rule, Passed, None, evaluatedMetrics)
            } else {
              RuleOutcome(rule, Failed,
                Some(s"Value: $ratio does not meet the constraint requirement."), evaluatedMetrics)
            }
          case None =>
            // No assertion provided, assume 1.0 threshold
            if (ratio == 1.0) {
              RuleOutcome(rule, Passed, None, evaluatedMetrics)
            } else {
              RuleOutcome(rule, Failed,
                Some(s"${(ratio * 100).formatted("%.2f")}% of rows passed"), evaluatedMetrics)
            }
        }
      }
    } match {
      case Success(result) => result
      case Failure(ex) =>
        RuleOutcome(
          rule,
          Failed,
          Some(s"Exception thrown while evaluating rule: ${ex.getMessage}")
        )
    }
  }
}
