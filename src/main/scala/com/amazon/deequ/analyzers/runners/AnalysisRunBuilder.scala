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
import org.apache.spark.sql.DataFrame

/** A class to build an AnalysisRun using a fluent API */
class AnalysisRunBuilder(data: DataFrame) {

  private[this] var analyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty

  protected[this] var metricsRepository: Option[MetricsRepository] = None

  protected[this] var reuseExistingResultsKey: Option[ResultKey] = None
  protected[this] var failIfResultsForReusingMissing: Boolean = false
  protected[this] var saveOrAppendResultsKey: Option[ResultKey] = None

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
    val builderWithRepository = new AnalysisRunBuilderWithRepository(data,
      Option(metricsRepository))
    builderWithRepository.addAnalyzers(analyzers)
  }

  def run(): AnalyzerContext = {
    AnalysisRunner.doAnalysisRun(
      data,
      analyzers,
      metricsRepository = metricsRepository,
      reuseExistingResultsForKey = reuseExistingResultsKey,
      failIfResultsForReusingMissing = failIfResultsForReusingMissing,
      saveOrAppendResultsWithKey = saveOrAppendResultsKey)
  }
}

class AnalysisRunBuilderWithRepository(
    data: DataFrame,
    usingMetricsRepository: Option[MetricsRepository])
  extends AnalysisRunBuilder(data) {

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
