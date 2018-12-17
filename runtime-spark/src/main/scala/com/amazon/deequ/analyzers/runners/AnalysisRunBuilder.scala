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

package com.amazon.deequ.analyzers.runners

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** A class to build an AnalysisRun using a fluent API */
class AnalysisRunBuilder(val data: DataFrame) {

  protected var analyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty

  protected var metricsRepository: Option[MetricsRepository] = None

  protected var reuseExistingResultsKey: Option[ResultKey] = None
  protected var failIfResultsForReusingMissing: Boolean = false
  protected var saveOrAppendResultsKey: Option[ResultKey] = None

  protected var sparkSession: Option[SparkSession] = None
  protected var saveSuccessMetricsJsonPath: Option[String] = None
  protected var overwriteOutputFiles: Boolean = false

  protected def this(analysisRunBuilder: AnalysisRunBuilder) {

    this(analysisRunBuilder.data)

    analyzers = analysisRunBuilder.analyzers

    metricsRepository = analysisRunBuilder.metricsRepository

    reuseExistingResultsKey = analysisRunBuilder.reuseExistingResultsKey
    failIfResultsForReusingMissing = analysisRunBuilder.failIfResultsForReusingMissing
    saveOrAppendResultsKey = analysisRunBuilder.saveOrAppendResultsKey

    sparkSession = analysisRunBuilder.sparkSession
    overwriteOutputFiles = analysisRunBuilder.overwriteOutputFiles
    saveSuccessMetricsJsonPath = analysisRunBuilder.saveSuccessMetricsJsonPath
  }

   /**
    * Add a single analyzer to the run.
    *
    * @param analyzer An analyzer to calculate a metric during the run
    */
  def addAnalyzer(analyzer: Analyzer[_, Metric[_]]): this.type = {
    analyzers :+= analyzer
    this
  }

  /**
    * Add multiple analyzers to the run.
    *
    * @param analyzers Analyzers to calculate metrics during the run
    */
  def addAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]]): this.type = {
    this.analyzers ++= analyzers
    this
  }

  /**
    * Set a metrics repository associated with the current data to enable features like reusing
    * previously computed results and storing the results of the current run.
    *
    * @param metricsRepository A metrics repository to store and load results associated with the
    *                          run
    */
  def useRepository(metricsRepository: MetricsRepository): AnalysisRunBuilderWithRepository = {

    new AnalysisRunBuilderWithRepository(this, Option(metricsRepository))
  }

  /**
    * Use a sparkSession to conveniently create output files
    *
    * @param sparkSession The SparkSession
    */
  def useSparkSession(
      sparkSession: SparkSession)
    : AnalysisRunBuilderWithSparkSession = {

    new AnalysisRunBuilderWithSparkSession(this, Option(sparkSession))
  }

  def run(): AnalyzerContext = {
    AnalysisRunner.doAnalysisRun(
      data,
      analyzers,
      metricsRepositoryOptions = AnalysisRunnerRepositoryOptions(
        metricsRepository,
        reuseExistingResultsKey,
        failIfResultsForReusingMissing,
        saveOrAppendResultsKey
      ),
      fileOutputOptions = AnalysisRunnerFileOutputOptions(
        sparkSession,
        saveSuccessMetricsJsonPath,
        overwriteOutputFiles
      )
    )
  }
}

class AnalysisRunBuilderWithRepository(
    analysisRunBuilder: AnalysisRunBuilder,
    usingMetricsRepository: Option[MetricsRepository])
  extends AnalysisRunBuilder(analysisRunBuilder) {

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
}

class AnalysisRunBuilderWithSparkSession(
    analysisRunBuilder: AnalysisRunBuilder,
    usingSparkSession: Option[SparkSession])
  extends AnalysisRunBuilder(analysisRunBuilder) {

  sparkSession = usingSparkSession

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
