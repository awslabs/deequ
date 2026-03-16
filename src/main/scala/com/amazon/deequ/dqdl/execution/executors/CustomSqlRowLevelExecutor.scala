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

package com.amazon.deequ.dqdl.execution.executors

import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{CustomSqlRowLevelExecutableRule, Failed, Passed, RuleOutcome, SingularColumn}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}
import software.amazon.glue.dqdl.model.DQRule

import scala.util.{Failure, Success, Try}

case class CustomSqlExecutionResult(outcomes: Map[DQRule, RuleOutcome], rowLevelData: DataFrame)

object CustomSqlRowLevelExecutor extends DQDLExecutor.RuleExecutor[CustomSqlRowLevelExecutableRule] {

  private val PrimaryAlias = "primary"
  private val MatchMarker = "__dq_match_marker"

  override def executeRules(rules: Seq[CustomSqlRowLevelExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    executeWithRowLevel(rules, df, additionalDataSources).outcomes
  }

  def executeWithRowLevel(rules: Seq[CustomSqlRowLevelExecutableRule], df: DataFrame,
                          additionalDataSources: Map[String, DataFrame] = Map.empty,
                          baseRowLevelData: Option[DataFrame] = None): CustomSqlExecutionResult = {
    if (rules.isEmpty) {
      return CustomSqlExecutionResult(Map.empty, baseRowLevelData.getOrElse(df))
    }

    val allSources = additionalDataSources + (PrimaryAlias -> df)
    allSources.foreach { case (alias, ds) => ds.createOrReplaceTempView(alias) }

    var currentData = baseRowLevelData.getOrElse(df)
    val outcomes = rules.map { rule =>
      val result = evaluateRule(rule, currentData, df.sparkSession)
      currentData = result.rowLevelData
      rule.dqRule -> result.outcome
    }

    CustomSqlExecutionResult(outcomes.toMap, currentData)
  }

  private case class SingleRuleResult(outcome: RuleOutcome, rowLevelData: DataFrame)

  private def evaluateRule(rule: CustomSqlRowLevelExecutableRule,
                           df: DataFrame,
                           sparkSession: SparkSession): SingleRuleResult = {
    val totalCount = df.count()
    if (totalCount == 0) {
      return SingleRuleResult(
        RuleOutcome(rule.dqRule, Failed,
          Some("Custom SQL rule could not be evaluated due to data frame being empty")),
        df)
    }

    Try(sparkSession.sql(rule.customSqlStatement)) match {
      case Failure(e) =>
        val colName = java.util.UUID.randomUUID().toString
        SingleRuleResult(
          RuleOutcome(rule.dqRule, Failed,
            Some(s"Error executing SQL statement: ${e.getMessage}"),
            rowLevelOutcome = SingularColumn(colName)),
          df.withColumn(colName, lit(false)))
      case Success(sqlDf) =>
        evaluateWithSqlResult(rule, df, sqlDf, totalCount)
    }
  }

  private def evaluateWithSqlResult(rule: CustomSqlRowLevelExecutableRule,
                                    df: DataFrame,
                                    sqlDf: DataFrame,
                                    totalCount: Long): SingleRuleResult = {
    val sqlCols = sqlDf.columns.toSeq
    val dfCols = df.columns.toSet
    val commonCols = sqlCols.filter(dfCols.contains)

    if (commonCols.isEmpty) {
      val colName = java.util.UUID.randomUUID().toString
      return SingleRuleResult(
        RuleOutcome(rule.dqRule, Failed,
          Some("The output from CustomSQL must contain at least one column that matches the " +
            "input dataset. Ensure that matching columns are returned from the SQL."),
          rowLevelOutcome = SingularColumn(colName)),
        df.withColumn(colName, lit(false)))
    }

    val colsNotInDf = sqlCols.filterNot(dfCols.contains)
    if (colsNotInDf.nonEmpty) {
      val colName = java.util.UUID.randomUUID().toString
      return SingleRuleResult(
        RuleOutcome(rule.dqRule, Failed,
          Some(s"The columns returned from the SQL statement should only belong to the primary " +
            s"table. Columns not found: ${colsNotInDf.mkString(", ")}"),
          rowLevelOutcome = SingularColumn(colName)),
        df.withColumn(colName, lit(false)))
    }

    Try {
      val colName = java.util.UUID.randomUUID().toString
      val renamedCols = sqlCols.map(c => c -> s"${c}__dq_renamed").toMap
      val renamedSqlDf = renamedCols.foldLeft(sqlDf) { case (d, (orig, renamed)) =>
        d.withColumnRenamed(orig, renamed)
      }.withColumn(MatchMarker, lit(true))

      val joinExpr = sqlCols.map { c =>
        when(df(c).isNull && renamedSqlDf(renamedCols(c)).isNull, lit(true))
          .otherwise(df(c) === renamedSqlDf(renamedCols(c)))
      }.reduce(_ && _)

      val joined = df.join(renamedSqlDf, joinExpr, "left")
      val augmented = joined
        .withColumn(colName, when(col(MatchMarker) === true, lit(true)).otherwise(lit(false)))
        .drop(renamedCols.values.toSeq :+ MatchMarker: _*)

      val matchedCount = augmented.filter(col(colName) === true).count()
      val ratio = matchedCount.toDouble / totalCount
      val metrics = Map("Dataset.*.CustomSQL.Compliance" -> ratio)

      val outcome = if (rule.assertion(ratio)) {
        RuleOutcome(rule.dqRule, Passed, None, metrics, rowLevelOutcome = SingularColumn(colName))
      } else {
        RuleOutcome(rule.dqRule, Failed,
          Some(s"Value: $ratio does not meet the constraint requirement."), metrics,
          rowLevelOutcome = SingularColumn(colName))
      }

      SingleRuleResult(outcome, augmented)
    } match {
      case Success(result) => result
      case Failure(e) =>
        val colName = java.util.UUID.randomUUID().toString
        SingleRuleResult(
          RuleOutcome(rule.dqRule, Failed,
            Some(s"Error evaluating Custom SQL row-level rule: ${e.getMessage}"),
            rowLevelOutcome = SingularColumn(colName)),
          df.withColumn(colName, lit(false)))
    }
  }
}
