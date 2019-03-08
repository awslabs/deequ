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

package com.amazon.deequ.runtime

import com.amazon.deequ.ComputedStatistics
import com.amazon.deequ.profiles.ColumnProfiles
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.statistics.Statistic

trait Engine {

  def compute(
      data: Dataset,
      statistics: Seq[Statistic],
      aggregateWith: Option[StateLoader] = None,
      saveStatesWith: Option[StatePersister] = None,
      engineRepositoryOptions: EngineRepositoryOptions = EngineRepositoryOptions())
//                                    fileOutputOptions: AnalysisRunnerFileOutputOptions =
//                                    AnalysisRunnerFileOutputOptions())
    : ComputedStatistics

  def profile(
      dataset: Dataset,
      restrictToColumns: Option[Seq[String]],
      lowCardinalityHistogramThreshold: Int,
      printStatusUpdates: Boolean,
      metricsRepository: Option[MetricsRepository],
      reuseExistingResultsUsingKey: Option[ResultKey],
      failIfResultsForReusingMissing: Boolean,
      saveInMetricsRepositoryUsingKey: Option[ResultKey])
    : ColumnProfiles

  def splitTrainTestSets(
      data: Dataset,
      testsetRatio: Option[Double],
      testsetSplitRandomSeed: Option[Long])
    : (Dataset, Option[Dataset])

}


private[deequ] case class EngineRepositoryOptions(
    metricsRepository: Option[MetricsRepository] = None,
    reuseExistingResultsForKey: Option[ResultKey] = None,
    failIfResultsForReusingMissing: Boolean = false,
    saveOrAppendResultsWithKey: Option[ResultKey] = None
)