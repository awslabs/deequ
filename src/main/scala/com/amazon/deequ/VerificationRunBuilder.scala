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

import com.amazon.deequ.anomalydetection.AnomalyDetectionStrategy
import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.{State, _}
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** A class to build a VerificationRun using a fluent API */
class VerificationRunBuilder(val data: DataFrame) {

  protected var requiredAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty

  protected var checks: Seq[Check] = Seq.empty

  protected var metricsRepository: Option[MetricsRepository] = None

  protected var reuseExistingResultsKey: Option[ResultKey] = None
  protected var failIfResultsForReusingMissing: Boolean = false
  protected var saveOrAppendResultsKey: Option[ResultKey] = None

  protected var sparkSession: Option[SparkSession] = None
  protected var saveCheckResultsJsonPath: Option[String] = None
  protected var saveSuccessMetricsJsonPath: Option[String] = None
  protected var overwriteOutputFiles: Boolean = false

  protected var statePersister: Option[StatePersister] = None
  protected var stateLoader: Option[StateLoader] = None

  protected def this(verificationRunBuilder: VerificationRunBuilder) {

    this(verificationRunBuilder.data)

    requiredAnalyzers = verificationRunBuilder.requiredAnalyzers

    checks = verificationRunBuilder.checks

    metricsRepository = verificationRunBuilder.metricsRepository

    reuseExistingResultsKey = verificationRunBuilder.reuseExistingResultsKey
    failIfResultsForReusingMissing = verificationRunBuilder.failIfResultsForReusingMissing
    saveOrAppendResultsKey = verificationRunBuilder.saveOrAppendResultsKey

    sparkSession = verificationRunBuilder.sparkSession
    overwriteOutputFiles = verificationRunBuilder.overwriteOutputFiles
    saveCheckResultsJsonPath = verificationRunBuilder.saveCheckResultsJsonPath
    saveSuccessMetricsJsonPath = verificationRunBuilder.saveSuccessMetricsJsonPath

    stateLoader = verificationRunBuilder.stateLoader
    statePersister = verificationRunBuilder.statePersister
  }

  /**
    * Add a single check to the run.
    *
    * @param check A check object to be executed during the run
    */
  def addCheck(check: Check): this.type = {
    checks :+= check
    this
  }

  /**
    * Add multiple checks to the run.
    *
    * @param checks A sequence of check objects to be executed during the run
    */
  def addChecks(checks: Seq[Check]): this.type = {
    this.checks ++= checks
    this
  }

  /**
    * Save analyzer states.
    * Enables aggregate computation of metrics later, e.g., when a new partition is
    * added to the dataset.
    *
    * @param statePersister A state persister that saves the computed states for later aggregation
    */
  def saveStatesWith(statePersister: StatePersister): this.type = {
    this.statePersister = Option(statePersister)
    this
  }

  /**
    * Use to load saved analyzer states and aggregate them with those calculated in this new run.
    * Can be used to efficiently compute metrics for a large dataset
    * if e.g. a new partition is added.
    *
    * @param stateLoader A state loader that loads previously calculated states and
    *                    allows aggregation with the ones calculated in this run.
    */
  def aggregateWith(stateLoader: StateLoader): this.type = {
    this.stateLoader = Option(stateLoader)
    this
  }

  /**
    * Can be used to enforce the calculation of some some metric regardless of if there is a
    * constraint on it (optional)
    *
    * @param requiredAnalyzer The analyzer to be used to calculate the metric during the run
    */
  def addRequiredAnalyzer(requiredAnalyzer: Analyzer[_, Metric[_]]): this.type = {
    requiredAnalyzers :+= requiredAnalyzer
    this
  }

   /**
    * Can be used to enforce the calculation of some some metrics regardless of if there are
    * constraints on them (optional)
    *
    * @param requiredAnalyzers The analyzers to be used to calculate the metrics during the run
    */
  def addRequiredAnalyzers(requiredAnalyzers: Seq[Analyzer[_, Metric[_]]]): this.type = {
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
  def useRepository(metricsRepository: MetricsRepository): VerificationRunBuilderWithRepository = {

    new VerificationRunBuilderWithRepository(this, Option(metricsRepository))
  }

  /**
    * Use a sparkSession to conveniently create output files
    *
    * @param sparkSession The SparkSession
    */
  def useSparkSession(
      sparkSession: SparkSession)
    : VerificationRunBuilderWithSparkSession = {

    new VerificationRunBuilderWithSparkSession(this, Option(sparkSession))
  }


  def run(): VerificationResult = {
    VerificationSuite().doVerificationRun(
      data,
      checks,
      requiredAnalyzers,
      metricsRepositoryOptions = VerificationMetricsRepositoryOptions(
        metricsRepository,
        reuseExistingResultsKey,
        failIfResultsForReusingMissing,
        saveOrAppendResultsKey),
      fileOutputOptions = VerificationFileOutputOptions(
        sparkSession,
        saveCheckResultsJsonPath,
        saveSuccessMetricsJsonPath,
        overwriteOutputFiles),
      saveStatesWith = statePersister,
      aggregateWith = stateLoader
    )
  }
}

class VerificationRunBuilderWithRepository(
    verificationRunBuilder: VerificationRunBuilder,
    usingMetricsRepository: Option[MetricsRepository])
  extends VerificationRunBuilder(verificationRunBuilder) {

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
      analyzer: Analyzer[S, Metric[Double]],
      anomalyCheckConfig: Option[AnomalyCheckConfig] = None)
    : this.type = {

    val anomalyCheckConfigOrDefault = anomalyCheckConfig.getOrElse {

      val checkDescription = s"Anomaly check for ${analyzer.toString}"

      AnomalyCheckConfig(CheckLevel.Warning, checkDescription)
    }

    checks :+= VerificationRunBuilderHelper.getAnomalyCheck(metricsRepository.get,
      anomalyDetectionStrategy, analyzer, anomalyCheckConfigOrDefault)
    this
  }
}

class VerificationRunBuilderWithSparkSession(
    verificationRunBuilder: VerificationRunBuilder,
    usingSparkSession: Option[SparkSession])
  extends VerificationRunBuilder(verificationRunBuilder) {

  sparkSession = usingSparkSession

  /**
    * Save the check results json to e.g. S3
    *
    * @param path The file path
    */
  def saveCheckResultsJsonToPath(
      path: String)
    : this.type = {

    saveCheckResultsJsonPath = Option(path)
    this
  }

  /**
    * Save the success metrics json to e.g. S3
    *
    * @param path The file path
    */
  def saveSuccessMetricsJsonToPath(
      path: String)
    : this.type = {

    saveSuccessMetricsJsonPath = Option(path)
    this
  }

  /**
    * Whether previous files with identical names should be overwritten when
    * saving files to some file system.
    *
    * @param overwriteFiles Whether previous files with identical names
    *                       should be overwritten
    */
  def overwritePreviousFiles(overwriteFiles: Boolean): this.type = {
    overwriteOutputFiles = overwriteOutputFiles
    this
  }
}

/** A class to build an AnomalyCheck  */
private[this] object VerificationRunBuilderHelper {

  /**
    * Build a check using Anomaly Detection methods
    * @param metricsRepository A metrics repository to get the previous results
    * @param anomalyDetectionStrategy The anomaly detection strategy
    * @param analyzer The analyzer for the metric to run anomaly detection on
    * @param anomalyCheckConfig Some configuration settings for the Check
    */
  def getAnomalyCheck[S <: State[S]](
      metricsRepository: MetricsRepository,
      anomalyDetectionStrategy: AnomalyDetectionStrategy,
      analyzer: Analyzer[S, Metric[Double]],
      anomalyCheckConfig: AnomalyCheckConfig)
    : Check = {

    Check(anomalyCheckConfig.level, anomalyCheckConfig.description)
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
case class AnomalyCheckConfig(
  level: CheckLevel.Value,
  description: String,
  withTagValues: Map[String, String] = Map.empty,
  afterDate: Option[Long] = None,
  beforeDate: Option[Long] = None)
