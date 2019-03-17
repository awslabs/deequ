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
class AnalysisRunBuilder[T](val data: Dataset[T]) {

  protected var statistics: Seq[Statistic] = Seq.empty

  protected var metricsRepository: Option[MetricsRepository] = None

  protected var reuseExistingResultsKey: Option[ResultKey] = None
  protected var failIfResultsForReusingMissing: Boolean = false
  protected var saveOrAppendResultsKey: Option[ResultKey] = None

  protected var saveSuccessMetricsJsonPath: Option[String] = None
  protected var overwriteOutputFiles: Boolean = false

  protected var aggregateWith: Option[StateLoader[T]] = None
  protected var saveStatesWith: Option[StatePersister[T]] = None

  protected def this(analysisRunBuilder: AnalysisRunBuilder[T]) {

    this(analysisRunBuilder.data)

    statistics = analysisRunBuilder.statistics

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
    * @param statistic An analyzer to calculate a metric during the run
    */
  def addStatistic(statistic: Statistic): this.type = {
    statistics :+= statistic
    this
  }

  /**
    * Add multiple analyzers to the run.
    *
    * @param statistics Analyzers to calculate metrics during the run
    */
  def addStatistics(statistics: Seq[Statistic]): this.type = {
    this.statistics ++= statistics
    this
  }

  def aggregateWith(stateLoader: StateLoader[T]): this.type = {
    this.aggregateWith = Some(stateLoader)
    this
  }

  def saveStatesWith(statePersister: StatePersister[T]): this.type = {
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
  def useRepository(metricsRepository: MetricsRepository): AnalysisRunBuilderWithRepository[T] = {
    new AnalysisRunBuilderWithRepository(this, Option(metricsRepository))
  }

  def run(): ComputedStatistics = {
    data.engine.compute(
      data,
      statistics,
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
  def onData[T](dataset: Dataset[T]): AnalysisRunBuilder[T] = {
    new AnalysisRunBuilder(dataset)
  }
}

class AnalysisRunBuilderWithRepository[T](
    analysisRunBuilder: AnalysisRunBuilder[T],
    usingMetricsRepository: Option[MetricsRepository])
  extends AnalysisRunBuilder[T](analysisRunBuilder) {

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
