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

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.constraints.{RowLevelAssertedConstraint, RowLevelConstraint, RowLevelGroupedConstraint}
import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{DeequExecutableRule, DeequMetricMapping, Failed, NoColumn, RuleOutcome, SingularColumn}
import com.amazon.deequ.dqdl.translation.rules.DuplicateRowCountRule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit}
import scala.collection.JavaConverters._
import software.amazon.glue.dqdl.model.DQRule

case class DeequExecutionResult(outcomes: Map[DQRule, RuleOutcome], rowLevelData: DataFrame)

object DeequRulesExecutor extends DQDLExecutor.RuleExecutor[DeequExecutableRule] {
  private val delim = "."

  override def executeRules(rules: Seq[DeequExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    executeWithRowLevel(rules, df).outcomes
  }

  def executeWithRowLevel(rules: Seq[DeequExecutableRule],
                          df: DataFrame): DeequExecutionResult = {
    if (rules.isEmpty) {
      return DeequExecutionResult(Map.empty, df)
    }

    // Resolve empty DuplicateRowCount columns to all DataFrame columns
    // so RowLevelGroupedConstraint gets a populated column list for row-level results
    val resolvedRules = rules.map(rule => resolveDuplicateRowCountColumns(rule, df))

    val verificationResult = VerificationSuite()
      .onData(df.toDF())
      .addChecks(resolvedRules.map(_.check))
      .run()

    val rowLevelData = VerificationResult.rowLevelResultsAsDataFrame(
      df.sparkSession, verificationResult, df)

    val metricsDF = VerificationResult.successMetricsAsDataFrame(df.sparkSession, verificationResult)
    val deequMetricMap = metricsDF.select(
      concat(col("entity"), lit(delim), col("instance"), lit(delim), col("name")).as("key"),
      col("value")
    ).collect().map { row =>
      row.getAs[String]("key") -> row.getAs[Double]("value")
    }.toMap

    val rowLevelCheckDescriptions = verificationResult.checkResults.flatMap {
      case (check, checkResult) =>
        val hasRowLevel = checkResult.constraintResults.exists { cr =>
          cr.constraint match {
            case _: RowLevelConstraint | _: RowLevelAssertedConstraint
                 | _: RowLevelGroupedConstraint => true
            case _ => false
          }
        }
        if (hasRowLevel) Some(check.description) else None
    }.toSet

    val outcomes = resolvedRules.map { r =>
      val rowLevelOutcome = if (rowLevelCheckDescriptions.contains(r.check.description)) {
        SingularColumn(r.check.description)
      } else {
        NoColumn()
      }

      val outcome = verificationResult.checkResults.get(r.check)
        .map { c =>
          RuleOutcome(r.dqRule, c).copy(
            evaluatedMetrics = extractEvaluatedMetrics(deequMetricMap, r.deequMetricMappings),
            rowLevelOutcome = rowLevelOutcome
          )
        }
        .getOrElse(RuleOutcome(r.dqRule, Failed, Some("Failed to check status"),
          rowLevelOutcome = rowLevelOutcome))

      r.dqRule -> outcome
    }.toMap

    DeequExecutionResult(outcomes, rowLevelData)
  }

  private[dqdl] def extractEvaluatedMetrics(deequMetricMap: Map[String, Double],
                                            deequMetricMappings: Seq[DeequMetricMapping]): Map[String, Double] = {
    deequMetricMappings.flatMap { mapping =>
      val instance = mapping.deequInstance.getOrElse(mapping.instance)
      val disambiguator = mapping.disambiguator.getOrElse("")
      val key = s"${mapping.entity}$delim$instance$delim${mapping.deequName} $disambiguator".trim
      deequMetricMap.get(key).map { metricValue =>
        s"${mapping.entity}$delim${mapping.instance}$delim${mapping.name}" -> metricValue
      }
    }.toMap
  }

  /**
   * Resolves empty columns in DuplicateRowCount rules to all DataFrame columns.
   * This ensures RowLevelGroupedConstraint gets a populated column list for row-level results.
   */
  private def resolveDuplicateRowCountColumns(rule: DeequExecutableRule, df: DataFrame): DeequExecutableRule = {
    import com.amazon.deequ.checks.{Check, CheckLevel}
    import com.amazon.deequ.dqdl.util.DQDLUtility.addWhereClause
    import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition

    // Only resolve for DuplicateRowCount with no explicit columns
    val isDuplicateRowCountNoColumns = rule.dqRule.getRuleType == "DuplicateRowCount" &&
      !rule.dqRule.getParameters.asScala.exists(_._1.startsWith("TargetColumn"))

    if (!isDuplicateRowCountNoColumns) {
      rule
    } else {
      // Re-derive the assertion from the DQRule condition
      val condition = rule.dqRule.getCondition.asInstanceOf[NumberBasedCondition]
      val converter = new DuplicateRowCountRule()
      val doubleAssertion = converter.assertionAsScala(rule.dqRule, condition)
      val longAssertion: Long => Boolean = (v: Long) => doubleAssertion(v.toDouble)

      // Rebuild check with all DataFrame columns
      val allColumns = df.columns.toSeq
      val check = Check(CheckLevel.Error, rule.check.description)
        .hasDuplicateRowCount(allColumns, longAssertion)

      val resolvedCheck = if (rule.dqRule.getWhereClause != null && !rule.dqRule.getWhereClause.isEmpty) {
        addWhereClause(rule.dqRule, check)
      } else {
        check
      }

      DeequExecutableRule(rule.dqRule, resolvedCheck, rule.deequMetricMappings)
    }
  }
}
