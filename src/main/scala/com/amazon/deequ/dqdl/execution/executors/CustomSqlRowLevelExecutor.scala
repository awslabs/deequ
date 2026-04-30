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

  def executeWithRowLevel(
      rules: Seq[CustomSqlRowLevelExecutableRule],
      df: DataFrame,
      additionalDataSources: Map[String, DataFrame] = Map.empty,
      baseRowLevelData: Option[DataFrame] = None
  ): CustomSqlExecutionResult = {
    if (rules.isEmpty) {
      return CustomSqlExecutionResult(
        Map.empty, baseRowLevelData.getOrElse(df))
    }

    val allSources = additionalDataSources + (PrimaryAlias -> df)
    allSources.foreach {
      case (alias, ds) => ds.createOrReplaceTempView(alias)
    }

    if (rules.size >= 2) {
      executeBatched(rules, df, baseRowLevelData)
    } else {
      executePerRule(rules, df, baseRowLevelData)
    }
  }

  private def executePerRule(
      rules: Seq[CustomSqlRowLevelExecutableRule],
      df: DataFrame,
      baseRowLevelData: Option[DataFrame]
  ): CustomSqlExecutionResult = {
    var currentData = baseRowLevelData.getOrElse(df)
    val outcomes = rules.map { rule =>
      val result = evaluateRule(rule, currentData, df.sparkSession)
      currentData = result.rowLevelData
      rule.dqRule -> result.outcome
    }
    CustomSqlExecutionResult(outcomes.toMap, currentData)
  }

  private def executeBatched(
      rules: Seq[CustomSqlRowLevelExecutableRule],
      df: DataFrame,
      baseRowLevelData: Option[DataFrame]
  ): CustomSqlExecutionResult = {
    Try {
      val startData = baseRowLevelData.getOrElse(df)
      val totalCount = startData.count().toDouble

      if (totalCount == 0) {
        val outcomes = rules.map { rule =>
          val cn = java.util.UUID.randomUUID().toString
          rule.dqRule -> RuleOutcome(rule.dqRule, Failed,
            Some("Custom SQL rule could not be evaluated " +
              "due to data frame being empty"),
            rowLevelOutcome = SingularColumn(cn))
        }.toMap
        val resultDf = rules.foldLeft(startData) { (acc, _) =>
          acc.withColumn(
            java.util.UUID.randomUUID().toString, lit(false))
        }
        return CustomSqlExecutionResult(outcomes, resultDf)
      }

      // Phase 1: join loop - accumulate boolean columns
      var currentData = startData
      var failedOutcomes = Map.empty[DQRule, RuleOutcome]
      val ruleColumns = rules.map { rule =>
        val colName = java.util.UUID.randomUUID().toString
        val joinResult = joinSqlResult(
          rule, currentData, df.sparkSession, colName)
        currentData = joinResult.data
        joinResult.failedOutcome.foreach { o =>
          failedOutcomes += (rule.dqRule -> o)
        }
        (rule, colName)
      }

      // Phase 2: single agg for valid rules
      val validRules = ruleColumns.filterNot {
        case (r, _) => failedOutcomes.contains(r.dqRule)
      }
      val metricsOutcomes: Map[DQRule, RuleOutcome] =
        if (validRules.nonEmpty) {
          import org.apache.spark.sql.functions.sum
          val aggExprs = validRules.map { case (_, cn) =>
            sum(when(col(s"`$cn`"), 1).otherwise(0))
              .cast("double").as(s"${cn}_count")
          }
          val counts = currentData
            .agg(aggExprs.head, aggExprs.tail: _*).head()

          validRules.map { case (rule, cn) =>
            val passed = counts.getAs[Double](s"${cn}_count")
            val ratio = passed / totalCount
            val metrics =
              Map("Dataset.*.CustomSQL.Compliance" -> ratio)
            val outcome = if (rule.assertion(ratio)) {
              RuleOutcome(rule.dqRule, Passed, None, metrics,
                rowLevelOutcome = SingularColumn(cn))
            } else {
              RuleOutcome(rule.dqRule, Failed,
                Some("Custom SQL response failed to " +
                  "satisfy the threshold"),
                metrics, rowLevelOutcome = SingularColumn(cn))
            }
            rule.dqRule -> outcome
          }.toMap
        } else Map.empty[DQRule, RuleOutcome]

      CustomSqlExecutionResult(
        failedOutcomes ++ metricsOutcomes, currentData)
    } match {
      case Success(r) => r
      case Failure(_) =>
        executePerRule(rules, df, baseRowLevelData)
    }
  }

  private case class JoinResult(
      data: DataFrame,
      failedOutcome: Option[RuleOutcome])

  private def joinSqlResult(
      rule: CustomSqlRowLevelExecutableRule,
      df: DataFrame,
      sparkSession: SparkSession,
      colName: String): JoinResult = {
    val rowLevel = SingularColumn(colName)

    Try(sparkSession.sql(rule.customSqlStatement)) match {
      case Failure(e) =>
        JoinResult(
          df.withColumn(colName, lit(false)),
          Some(RuleOutcome(rule.dqRule, Failed,
            Some(s"Error executing SQL statement: " +
              e.getMessage),
            rowLevelOutcome = rowLevel)))
      case Success(sqlDf) =>
        val sqlCols = sqlDf.columns.toSeq
        val dfCols = df.columns.toSet
        val common = sqlCols.filter(dfCols.contains)

        if (common.isEmpty) {
          JoinResult(
            df.withColumn(colName, lit(false)),
            Some(RuleOutcome(rule.dqRule, Failed,
              Some("The output from CustomSQL must contain " +
                "at least one column that matches the input " +
                "dataset. Ensure that matching columns are " +
                "returned from the SQL."),
              rowLevelOutcome = rowLevel)))
        } else {
          val extra = sqlCols.filterNot(dfCols.contains)
          if (extra.nonEmpty) {
            JoinResult(
              df.withColumn(colName, lit(false)),
              Some(RuleOutcome(rule.dqRule, Failed,
                Some("The columns returned from the SQL " +
                  "statement should only belong to the " +
                  "primary table. Columns not found: " +
                  extra.mkString(", ")),
                rowLevelOutcome = rowLevel)))
          } else {
            Try {
              val renamed = sqlCols.map(
                c => c -> s"${c}__dq_renamed").toMap
              val rSqlDf = renamed.foldLeft(sqlDf) {
                case (d, (o, r)) =>
                  d.withColumnRenamed(o, r)
              }.withColumn(MatchMarker, lit(true))

              val joinExpr = sqlCols.map { c =>
                when(df(c).isNull &&
                  rSqlDf(renamed(c)).isNull, lit(true))
                  .otherwise(df(c) === rSqlDf(renamed(c)))
              }.reduce(_ && _)

              val joined = df.join(rSqlDf, joinExpr, "left")
              joined
                .withColumn(colName,
                  when(col(MatchMarker) === true,
                    lit(true)).otherwise(lit(false)))
                .drop(
                  renamed.values.toSeq :+ MatchMarker: _*)
            } match {
              case Success(augmented) =>
                JoinResult(augmented, None)
              case Failure(e) =>
                JoinResult(
                  df.withColumn(colName, lit(false)),
                  Some(RuleOutcome(rule.dqRule, Failed,
                    Some("Error evaluating Custom SQL " +
                      "row-level rule: " + e.getMessage),
                    rowLevelOutcome = rowLevel)))
            }
          }
        }
    }
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
          Some(s"Custom SQL response failed to satisfy the threshold"), metrics,
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
