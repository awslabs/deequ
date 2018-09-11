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
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.SimpleResultSerde
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
