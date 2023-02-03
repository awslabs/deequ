/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.checks.{Check, CheckResult, CheckStatus}
import com.amazon.deequ.constraints.AnalysisBasedConstraint
import com.amazon.deequ.constraints.NamedConstraint
import com.amazon.deequ.constraints.RowLevelConstraint
import com.amazon.deequ.metrics.FullColumn
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.SimpleResultSerde
import org.apache.spark.sql.Column
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * The result returned from the VerificationSuite
  *
  * @param status The overall status of the Verification run
  * @param checkResults Checks and their CheckResults
  * @param metrics Analyzers and their Metric results
  */
case class VerificationResult(
    status: CheckStatus.Value,
    checkResults: Map[Check, CheckResult],
    metrics: Map[Analyzer[_, Metric[_]], Metric[_]])

object VerificationResult {

  def successMetricsAsDataFrame(
      sparkSession: SparkSession,
      verificationResult: VerificationResult,
      forAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty)
    : DataFrame = {

    val metricsAsAnalyzerContext = AnalyzerContext(verificationResult.metrics)

    AnalyzerContext.successMetricsAsDataFrame(sparkSession, metricsAsAnalyzerContext, forAnalyzers)
  }

  def successMetricsAsJson(verificationResult: VerificationResult,
    forAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty): String = {

    val metricsAsAnalyzerContext = AnalyzerContext(verificationResult.metrics)

    AnalyzerContext.successMetricsAsJson(metricsAsAnalyzerContext, forAnalyzers)
  }

  def checkResultsAsDataFrame(
      sparkSession: SparkSession,
      verificationResult: VerificationResult,
      forChecks: Seq[Check] = Seq.empty)
    : DataFrame = {

    val simplifiedCheckResults = getSimplifiedCheckResultOutput(verificationResult)

    import sparkSession.implicits._

    simplifiedCheckResults.toDF("check", "check_level", "check_status", "constraint",
      "constraint_status", "constraint_message")
  }

  /**
   * For each check in the verification suite, adds a column of row-level results
   * to the input data if that check contains a column.
   *
   * Accepts a naming rule
   */
  def toRowLevelResults(
      sparkSession: SparkSession,
      verificationResult: VerificationResult,
      data: DataFrame): DataFrame = {

    val columnNamesToMetrics: Map[String, Metric[_]] = mapResultToMetric(verificationResult)
      .flatMap(mapColumnNameToMetric)

    val checks: Seq[(String, Column)] = columnNamesToMetrics.flatMap(getColumnIfCalculated).toList

    checks.foldLeft(data)(
      (data, newColumn: (String, Column)) => data.withColumn(newColumn._1, newColumn._2))
  }

  def checkResultsAsJson(verificationResult: VerificationResult,
    forChecks: Seq[Check] = Seq.empty): String = {

    val simplifiedCheckResults = getSimplifiedCheckResultOutput(verificationResult)

    val checkResults = simplifiedCheckResults
      .map { simpleCheckResultOutput =>
          Map(
            "check" -> simpleCheckResultOutput.checkDescription,
            "check_level" -> simpleCheckResultOutput.checkLevel,
            "check_status" -> simpleCheckResultOutput.checkStatus,
            "constraint" -> simpleCheckResultOutput.constraint,
            "constraint_status" -> simpleCheckResultOutput.constraintStatus,
            "constraint_message" -> simpleCheckResultOutput.constraintMessage
          )
      }

    SimpleResultSerde.serialize(checkResults)
  }

  /**
   * This is necessary because in VerificationResult the order of CheckResult and Metric doesn't match
   */
  private def mapResultToMetric(verificationResult: VerificationResult): Map[CheckResult, Seq[Metric[_]]] = {
    val results: Map[Check, CheckResult] = verificationResult.checkResults
    results.map(r => r._2 -> r._2.constraintResults.flatMap(_.metric))
  }

  private def mapColumnNameToMetric(pair: (CheckResult, Seq[Metric[_]])): Option[(String, Metric[_])] = {
    val constraints = pair._1.check.constraints
    val metrics = pair._2
    // TODO: Aggregate all constraints with AND, don't just look at the first one
    constraints.head match {
      case c: RowLevelConstraint => Some((c.getColumnName, metrics.head))
      case _ => None
    }
  }

  private[this] def getColumnIfCalculated(pair: (String, Metric[_])): Option[(String, Column)] = {
    pair._2 match {
      case fullColumn: FullColumn =>
        if (fullColumn.fullColumn.isDefined) Some(pair._1, fullColumn.fullColumn.get) else None
      case _ => None
    }
  }

  private[this] def getSimplifiedCheckResultOutput(
      verificationResult: VerificationResult)
    : Seq[SimpleCheckResultOutput] = {

    val selectedCheckResults = verificationResult.checkResults
      .values
      .toSeq

    selectedCheckResults
      .flatMap { checkResult =>
        checkResult.constraintResults.map { constraintResult =>
          SimpleCheckResultOutput(
            checkResult.check.description,
            checkResult.check.level.toString,
            checkResult.status.toString,

            constraintResult.constraint.toString,
            constraintResult.status.toString,
            constraintResult.message.getOrElse("")
          )
        }
      }
  }

  private[this] case class SimpleCheckResultOutput(checkDescription: String, checkLevel: String,
    checkStatus: String, constraint: String, constraintStatus: String, constraintMessage: String)
}
