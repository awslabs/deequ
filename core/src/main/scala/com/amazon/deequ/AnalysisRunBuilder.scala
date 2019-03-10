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

import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.runtime._
import com.amazon.deequ.statistics.Statistic

/** A class to build an AnalysisRun using a fluent API */
class AnalysisRunBuilder(val data: Dataset, val engine: Engine) {

  protected var analyzers: Seq[Statistic] = Seq.empty

  protected var metricsRepository: Option[MetricsRepository] = None

  protected var reuseExistingResultsKey: Option[ResultKey] = None
  protected var failIfResultsForReusingMissing: Boolean = false
  protected var saveOrAppendResultsKey: Option[ResultKey] = None

  protected var saveSuccessMetricsJsonPath: Option[String] = None
  protected var overwriteOutputFiles: Boolean = false

  protected var aggregateWith: Option[StateLoader] = None
  protected var saveStatesWith: Option[StatePersister] = None

  protected def this(analysisRunBuilder: AnalysisRunBuilder) {

    this(analysisRunBuilder.data, analysisRunBuilder.engine)

    analyzers = analysisRunBuilder.analyzers

    metricsRepository = analysisRunBuilder.metricsRepository

    reuseExistingResultsKey = analysisRunBuilder.reuseExistingResultsKey
    failIfResultsForReusingMissing = analysisRunBuilder.failIfResultsForReusingMissing
    saveOrAppendResultsKey = analysisRunBuilder.saveOrAppendResultsKey

    overwriteOutputFiles = analysisRunBuilder.overwriteOutputFiles
    saveSuccessMetricsJsonPath = analysisRunBuilder.saveSuccessMetricsJsonPath
  }

   /**
    * Add a single analyzer to the run.
    *
    * @param analyzer An analyzer to calculate a metric during the run
    */
  def addAnalyzer(analyzer: Statistic): this.type = {
    analyzers :+= analyzer
    this
  }

  /**
    * Add multiple analyzers to the run.
    *
    * @param analyzers Analyzers to calculate metrics during the run
    */
  def addAnalyzers(analyzers: Seq[Statistic]): this.type = {
    this.analyzers ++= analyzers
    this
  }

  def aggregateWith(stateLoader: StateLoader): this.type = {
    this.aggregateWith = Some(stateLoader)
    this
  }

  def saveStatesWith(statePersister: StatePersister): this.type = {
    this.saveStatesWith = Some(statePersister)
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

  def run(): ComputedStatistics = {
    engine.compute(
      data,
      analyzers,
      engineRepositoryOptions = EngineRepositoryOptions(
        metricsRepository,
        reuseExistingResultsKey,
        failIfResultsForReusingMissing,
        saveOrAppendResultsKey
      ),
      aggregateWith = aggregateWith,
      saveStatesWith = saveStatesWith
    )
//      fileOutputOptions = AnalysisRunnerFileOutputOptions(
//        sparkSession,
//        saveSuccessMetricsJsonPath,
//        overwriteOutputFiles
//      )
  }
}

object Analysis {
  def onData(dataset: Dataset, engine: Engine): AnalysisRunBuilder = {
    new AnalysisRunBuilder(dataset, engine)
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
