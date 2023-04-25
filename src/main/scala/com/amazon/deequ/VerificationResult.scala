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
import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckResult
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.ConstraintResult
import com.amazon.deequ.constraints.RowLevelAssertedConstraint
import com.amazon.deequ.constraints.RowLevelConstraint
import com.amazon.deequ.constraints.RowLevelGroupedConstraint
import com.amazon.deequ.metrics.FullColumn
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.SimpleResultSerde
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

import java.util.UUID

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

  val UNIQUENESS_ID = UUID.randomUUID().toString

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
  def rowLevelResultsAsDataFrame(
      sparkSession: SparkSession,
      verificationResult: VerificationResult,
      data: DataFrame): DataFrame = {

    val columnNamesToMetrics: Map[String, Column] = verificationResultToColumn(verificationResult)

    val dataWithID = data.withColumn(UNIQUENESS_ID, monotonically_increasing_id())
    columnNamesToMetrics.foldLeft(dataWithID)(
      (dataWithID, newColumn: (String, Column)) => dataWithID.withColumn(newColumn._1, newColumn._2)).drop(UNIQUENESS_ID)
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
   * Returns a column for each check whose values are the result of each of the check's constraints
   */
  private def verificationResultToColumn(verificationResult: VerificationResult): Map[String, Column] = {
    verificationResult.checkResults.flatMap(pair => columnForCheckResult(pair._1, pair._2))
  }

  private def columnForCheckResult(check: Check, checkResult: CheckResult): Option[(String, Column)] = {
    // Convert non-boolean columns to boolean by using the assertion

    val metrics: Seq[Column] = checkResult.constraintResults.flatMap(constraintResultToColumn)
    if (metrics.isEmpty) {
      None
    } else {
      Some(check.description, metrics.reduce(_ and _))
    }
  }

  private def constraintResultToColumn(constraintResult: ConstraintResult): Option[Column] = {
    val constraint = constraintResult.constraint
    constraint match {
      case asserted: RowLevelAssertedConstraint =>
        constraintResult.metric.flatMap(metricToColumn).map(asserted.assertion(_))
      case _: RowLevelConstraint =>
        constraintResult.metric.flatMap(metricToColumn)
      case _: RowLevelGroupedConstraint =>
        constraintResult.metric.flatMap(metricToColumn)
      case _ => None
    }
  }

  private def metricToColumn(metric: Metric[_]): Option[Column] = {
    metric match {
      case fullColumn: FullColumn => fullColumn.fullColumn
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
