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

import com.amazon.deequ.checks.{Check, CheckResult, CheckStatus}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.statistics.Statistic

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
    metrics: Map[Statistic, Metric[_]]
)

//
//  def successMetricsAsDataFrame(
//                                 sparkSession: SparkSession,
//                                 verificationResult: VerificationResult,
//                                 forAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty)
//  : DataFrame = {
//
//    val metricsAsAnalyzerContext = AnalyzerContext(verificationResult.metrics)
//
//    AnalyzerContext.successMetricsAsDataFrame(sparkSession, metricsAsAnalyzerContext, forAnalyzers)
//  }
//

//
//  def checkResultsAsDataFrame(
//                               sparkSession: SparkSession,
//                               verificationResult: VerificationResult,
//                               forChecks: Seq[Check] = Seq.empty)
//  : DataFrame = {
//
//    val simplifiedCheckResults = getSimplifiedCheckResultOutput(verificationResult)
//
//    import sparkSession.implicits._
//
//    simplifiedCheckResults.toDF("check", "check_level", "check_status", "constraint",
//      "constraint_status", "constraint_message")
//  }
//

