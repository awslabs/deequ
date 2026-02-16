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
import com.amazon.deequ.dqdl.model.{RuleOutcome, Passed, Failed, SingularColumn}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import software.amazon.glue.dqdl.model.DQRule
import scala.util.{Try, Success, Failure}

case class RuleEvaluationResult(outcome: RuleOutcome, rowLevelColumnName: Option[String] = None)

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
    evaluateRuleWithRowLevel(df, rule, targetColumn, filteredRowOutcome, metricName,
      outcomeExpression, assertion).outcome
  }

  def evaluateRuleWithRowLevel(
    df: DataFrame,
    rule: DQRule,
    targetColumn: String,
    filteredRowOutcome: FilteredRowOutcome,
    metricName: String,
    outcomeExpression: Column,
    assertion: Option[Double => Boolean] = None
  ): RuleEvaluationResult = {

    val whereClause = Option(rule.getWhereClause)

    if (Try(df(targetColumn)).isFailure) {
      return RuleEvaluationResult(RuleOutcome(
        rule, Failed, Some(s"Column $targetColumn does not exist in the dataset")
      ))
    }

    val validatedFilteredDf = whereClause match {
      case Some(where) =>
        Try(df.where(where)).toOption match {
          case Some(filtered) => Some(filtered)
          case None =>
            return RuleEvaluationResult(RuleOutcome(
              rule, Failed, Some("The provided where clause is invalid")
            ))
        }
      case None => None
    }

    Try {
      val filteredDf = validatedFilteredDf.getOrElse(df)
      val totalCount = filteredDf.count()

      if (totalCount == 0) {
        RuleEvaluationResult(RuleOutcome(rule, Passed, Some("No rows matched the filter")))
      } else {
        val rowLevelColName = java.util.UUID.randomUUID().toString
        val augmentedDfWithResults = whereClause match {
          case Some(where) =>
            df.withColumn(rowLevelColName, when(not(expr(where)), null).otherwise(outcomeExpression))
          case None =>
            df.withColumn(rowLevelColName, outcomeExpression)
        }

        val filteredCount = augmentedDfWithResults.where(col(rowLevelColName) === true).count()
        val ratio = filteredCount.toDouble / totalCount

        val evaluatedMetrics = Map(metricName -> ratio)

        RowLevelDataTracker.addColumn(rowLevelColName, whereClause match {
          case Some(where) => when(not(expr(where)), null).otherwise(outcomeExpression)
          case None => outcomeExpression
        })

        val outcome = assertion match {
          case Some(assertFn) =>
            if (assertFn(ratio)) {
              RuleOutcome(rule, Passed, None, evaluatedMetrics)
            } else {
              RuleOutcome(rule, Failed,
                Some(s"Value: $ratio does not meet the constraint requirement."), evaluatedMetrics)
            }
          case None =>
            if (ratio == 1.0) {
              RuleOutcome(rule, Passed, None, evaluatedMetrics)
            } else {
              RuleOutcome(rule, Failed,
                Some(s"${(ratio * 100).formatted("%.2f")}% of rows passed"), evaluatedMetrics)
            }
        }

        RuleEvaluationResult(
          outcome.copy(rowLevelOutcome = SingularColumn(rowLevelColName)),
          Some(rowLevelColName)
        )
      }
    } match {
      case Success(result) => result
      case Failure(ex) =>
        RuleEvaluationResult(RuleOutcome(
          rule, Failed, Some(s"Exception thrown while evaluating rule: ${ex.getMessage}")
        ))
    }
  }
}
