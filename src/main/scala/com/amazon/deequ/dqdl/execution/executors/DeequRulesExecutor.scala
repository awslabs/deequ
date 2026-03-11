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

package com.amazon.deequ.dqdl.execution.executors

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.constraints.{RowLevelAssertedConstraint, RowLevelConstraint, RowLevelGroupedConstraint}
import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{DeequExecutableRule, DeequMetricMapping, Failed, NoColumn, RuleOutcome, SingularColumn}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit}
import software.amazon.glue.dqdl.model.DQRule

object DeequRulesExecutor extends DQDLExecutor.RuleExecutor[DeequExecutableRule] {
  private val delim = "."

  private val _lastRowLevelData = new ThreadLocal[Option[DataFrame]] {
    override def initialValue(): Option[DataFrame] = None
  }

  def lastRowLevelData: Option[DataFrame] = _lastRowLevelData.get()

  def resetRowLevelData(): Unit = _lastRowLevelData.set(None)

  override def executeRules(rules: Seq[DeequExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    val verificationResult = VerificationSuite()
      .onData(df.toDF())
      .addChecks(rules.map(_.check))
      .run()

    _lastRowLevelData.set(Some(
      VerificationResult.rowLevelResultsAsDataFrame(df.sparkSession, verificationResult, df)
    ))

    val metricsDF = VerificationResult.successMetricsAsDataFrame(df.sparkSession, verificationResult)

    val deequMetricMap = metricsDF.select(
      concat(col("entity"), lit(delim), col("instance"), lit(delim), col("name")).as("key"),
      col("value")
    ).collect().map { row =>
      val key = row.getAs[String]("key")
      val value = row.getAs[Double]("value")
      key -> value
    }.toMap

    val rowLevelCheckDescriptions: Set[String] = verificationResult.checkResults.flatMap {
      case (check, checkResult) =>
        val hasRowLevel = checkResult.constraintResults.exists { cr =>
          cr.constraint match {
            case _: RowLevelConstraint | _: RowLevelAssertedConstraint | _: RowLevelGroupedConstraint => true
            case _ => false
          }
        }
        if (hasRowLevel) Some(check.description) else None
    }.toSet

    rules.map { r =>
      val outcome = verificationResult.checkResults.get(r.check)
        .map { c =>
          val rowLevelOutcome = if (rowLevelCheckDescriptions.contains(r.check.description)) {
            SingularColumn(r.check.description)
          } else {
            NoColumn()
          }
          RuleOutcome(r.dqRule, c).copy(
            evaluatedMetrics = extractEvaluatedMetrics(deequMetricMap, r.deequMetricMappings),
            rowLevelOutcome = rowLevelOutcome
          )
        }
        .getOrElse(RuleOutcome(r.dqRule, Failed, failureReason = Some("Failed to check status")))

      r.dqRule -> outcome
    }.toMap
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

}
