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

import com.amazon.deequ.analyzers.State
import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzer, Table}
import com.amazon.deequ.anomalydetection.AnomalyDetectionStrategy
import com.amazon.deequ.checks.{JdbcCheck, JdbcCheckLevel}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository._

/** A class to build a VerificationRun using a fluent API */
class JdbcVerificationRunBuilder(val data: Table) {

  protected var requiredAnalyzers: Seq[JdbcAnalyzer[_, Metric[_]]] = Seq.empty

  protected var checks: Seq[JdbcCheck] = Seq.empty

  protected var metricsRepository: Option[JdbcMetricsRepository] = None

  protected var reuseExistingResultsKey: Option[ResultKey] = None
  protected var failIfResultsForReusingMissing: Boolean = false
  protected var saveOrAppendResultsKey: Option[ResultKey] = None

  protected var saveCheckResultsJsonPath: Option[String] = None
  protected var saveSuccessMetricsJsonPath: Option[String] = None
  protected var overwriteOutputFiles: Boolean = false

  protected def this(verificationRunBuilder: JdbcVerificationRunBuilder) {

    this(verificationRunBuilder.data)

    requiredAnalyzers = verificationRunBuilder.requiredAnalyzers

    checks = verificationRunBuilder.checks

    metricsRepository = verificationRunBuilder.metricsRepository

    reuseExistingResultsKey = verificationRunBuilder.reuseExistingResultsKey
    failIfResultsForReusingMissing = verificationRunBuilder.failIfResultsForReusingMissing
    saveOrAppendResultsKey = verificationRunBuilder.saveOrAppendResultsKey

    overwriteOutputFiles = verificationRunBuilder.overwriteOutputFiles
    saveCheckResultsJsonPath = verificationRunBuilder.saveCheckResultsJsonPath
    saveSuccessMetricsJsonPath = verificationRunBuilder.saveSuccessMetricsJsonPath
  }

  /**
    * Add a single check to the run.
    *
    * @param check A check object to be executed during the run
    */
  def addCheck(check: JdbcCheck): this.type = {
    checks :+= check
    this
  }

  /**
    * Add multiple checks to the run.
    *
    * @param checks A sequence of check objects to be executed during the run
    */
  def addChecks(checks: Seq[JdbcCheck]): this.type = {
    this.checks ++= checks
    this
  }

  /**
    * Can be used to enforce the calculation of some some metric regardless of if there is a
    * constraint on it (optional)
    *
    * @param requiredAnalyzer The analyzer to be used to calculate the metric during the run
    */
  def addRequiredAnalyzer(requiredAnalyzer: JdbcAnalyzer[_, Metric[_]]): this.type = {
    requiredAnalyzers :+= requiredAnalyzer
    this
  }

   /**
    * Can be used to enforce the calculation of some some metrics regardless of if there are
    * constraints on them (optional)
    *
    * @param requiredAnalyzers The analyzers to be used to calculate the metrics during the run
    */
  def addRequiredAnalyzers(requiredAnalyzers: Seq[JdbcAnalyzer[_, Metric[_]]]): this.type = {
    this.requiredAnalyzers ++= requiredAnalyzers
    this
  }

  /**
    * Set a metrics repository associated with the current data to enable features like reusing
    * previously computed results and storing the results of the current run.
    *
    * @param metricsRepository A metrics repository to store and load results associated with the
    *                          run
    */
  def useRepository(metricsRepository: JdbcMetricsRepository):
  JdbcVerificationRunBuilderWithRepository = {

    new JdbcVerificationRunBuilderWithRepository(this, Option(metricsRepository))
  }

  def run(): JdbcVerificationResult = {
    JdbcVerificationSuite().doVerificationRun(
      data,
      checks,
      requiredAnalyzers,
      metricsRepositoryOptions = JdbcVerificationMetricsRepositoryOptions(
        metricsRepository,
        reuseExistingResultsKey,
        failIfResultsForReusingMissing,
        saveOrAppendResultsKey),
      fileOutputOptions = JdbcVerificationFileOutputOptions(
        saveCheckResultsJsonPath,
        saveSuccessMetricsJsonPath,
        overwriteOutputFiles)
    )
  }
}

class JdbcVerificationRunBuilderWithRepository(
    verificationRunBuilder: JdbcVerificationRunBuilder,
    usingMetricsRepository: Option[JdbcMetricsRepository])
  extends JdbcVerificationRunBuilder(verificationRunBuilder) {

  metricsRepository = usingMetricsRepository

  /**
    * Reuse any previously computed results stored in the metrics repository associated with the
    * current data to save computation time.
    *
    * @param resultKey The exact result key of the previously computed result
    * @param failIfResultsMissing Whether the run should fail if new metric calculations are needed
    */
  def reuseExistingResultsForKey(
      resultKey: ResultKey,
      failIfResultsMissing: Boolean = false)
    : this.type = {

    reuseExistingResultsKey = Option(resultKey)
    failIfResultsForReusingMissing = failIfResultsMissing
    this
  }

  /**
    * A shortcut to save the results of the run or append them to existing results in the
    * metrics repository.
    *
    * @param resultKey The result key to identify the current run
    */
  def saveOrAppendResult(resultKey: ResultKey): this.type = {
    saveOrAppendResultsKey = Option(resultKey)
    this
  }

  /**
    * Add a check using Anomaly Detection methods. The Anomaly Detection Strategy only checks
    * if the new value is an Anomaly.
    *
    * @param anomalyDetectionStrategy The anomaly detection strategy
    * @param analyzer The analyzer for the metric to run anomaly detection on
    * @param anomalyCheckConfig Some configuration settings for the Check
    */
  def addAnomalyCheck[S <: State[S]](
      anomalyDetectionStrategy: AnomalyDetectionStrategy,
      analyzer: JdbcAnalyzer[S, Metric[Double]],
      anomalyCheckConfig: Option[JdbcAnomalyCheckConfig] = None)
    : this.type = {

    val anomalyCheckConfigOrDefault = anomalyCheckConfig.getOrElse {

      val checkDescription = s"Anomaly check for ${analyzer.toString}"

      JdbcAnomalyCheckConfig(JdbcCheckLevel.Warning, checkDescription)
    }

    checks :+= JdbcVerificationRunBuilderHelper.getAnomalyCheck(metricsRepository.get,
      anomalyDetectionStrategy, analyzer, anomalyCheckConfigOrDefault)
    this
  }
}

/** A class to build an AnomalyCheck  */
private[this] object JdbcVerificationRunBuilderHelper {

  /**
    * Build a check using Anomaly Detection methods
    * @param metricsRepository A metrics repository to get the previous results
    * @param anomalyDetectionStrategy The anomaly detection strategy
    * @param analyzer The analyzer for the metric to run anomaly detection on
    * @param anomalyCheckConfig Some configuration settings for the Check
    */
  def getAnomalyCheck[S <: State[S]](
      metricsRepository: JdbcMetricsRepository,
      anomalyDetectionStrategy: AnomalyDetectionStrategy,
      analyzer: JdbcAnalyzer[S, Metric[Double]],
      anomalyCheckConfig: JdbcAnomalyCheckConfig)
    : JdbcCheck = {

    JdbcCheck(anomalyCheckConfig.level, anomalyCheckConfig.description)
      .isNewestPointNonAnomalous(
        metricsRepository,
        anomalyDetectionStrategy,
        analyzer,
        anomalyCheckConfig.withTagValues,
        anomalyCheckConfig.afterDate,
        anomalyCheckConfig.beforeDate
      )
  }
}

/**
    * Configuration for an anomaly check
    *
    * @param level         Assertion level of the check group. If any of the constraints fail this
    *                      level is used for the status of the check.
    * @param description   The name describes the check block. Generally will be used to show in
    *                      the logs.
    * @param withTagValues Can contain a Map with tag names and the corresponding values to filter
    *                      for the Anomaly Detection
    * @param afterDate     The minimum dateTime of previous AnalysisResults to use for the
    *                      Anomaly Detection
    * @param beforeDate    The maximum dateTime of previous AnalysisResults to use for the
    *                      Anomaly Detection
  * @return
    */
case class JdbcAnomalyCheckConfig(
  level: JdbcCheckLevel.Value,
  description: String,
  withTagValues: Map[String, String] = Map.empty,
  afterDate: Option[Long] = None,
  beforeDate: Option[Long] = None)
