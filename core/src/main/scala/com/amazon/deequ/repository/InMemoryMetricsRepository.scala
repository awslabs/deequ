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

package com.amazon.deequ.repository

import java.util.concurrent.ConcurrentHashMap

import com.amazon.deequ.ComputedStatistics
import com.amazon.deequ.statistics.Statistic

import scala.collection.JavaConversions._

/** A simple Repository implementation backed by a concurrent hash map */
class InMemoryMetricsRepository() extends MetricsRepository {

  private val resultsRepository = new ConcurrentHashMap[ResultKey, StatisticsResult]()

  /**
    * Saves Analysis results (metrics)
    *
    * @param resultKey represents the version of the dataset deequ checks were run on.
    * @param computedStatistics
    */
  def save(resultKey: ResultKey, computedStatistics: ComputedStatistics): Unit = {

    val successfulMetrics = computedStatistics.metricMap.filter {
      case (_, metric) => metric.value.isSuccess
    }

    val analyzerContextWithSuccessfulValues = ComputedStatistics(successfulMetrics)

    resultsRepository.put(resultKey, StatisticsResult(resultKey, analyzerContextWithSuccessfulValues))
  }

  /**
    * Get ComputedStatistics saved using exactly the same resultKey if present
    *
    * @param resultKey represents the version of the dataset deequ checks were run on.
    */
  def loadByKey(resultKey: ResultKey): Option[ComputedStatistics] = {
    Option(resultsRepository.get(resultKey)).map { _.computedStatistics }
  }

  /** Get a builder class to construct a loading query to get AnalysisResults */
  def load(): MetricsRepositoryMultipleResultsLoader = {
    new LimitedInMemoryMetricsRepositoryMultipleResultsLoader(resultsRepository)
  }
}

class LimitedInMemoryMetricsRepositoryMultipleResultsLoader(
    resultsRepository:
    ConcurrentHashMap[ResultKey, StatisticsResult])
  extends MetricsRepositoryMultipleResultsLoader {


  private[this] var tagValues: Option[Map[String, String]] = None
  private[this] var forAnalyzers: Option[Seq[Statistic]] = None
  private[this] var before: Option[Long] = None
  private[this] var after: Option[Long] = None

  /**
    * Filter out results that don't have specific values for specific tags
    *
    * @param tagValues Map with tag names and the corresponding values to filter for
    */
  def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader = {
    this.tagValues = Option(tagValues)
    this
  }

  /**
    * Choose all metrics that you want to load
    *
    * @param analyzers A sequence of analyers who's resulting metrics you want to load
    */
  def forStatistics(analyzers: Seq[Statistic])
  : MetricsRepositoryMultipleResultsLoader = {

    this.forAnalyzers = Option(analyzers)
    this
  }

  /**
    * Only look at AnalysisResults with a history key with a smaller value
    *
    * @param dateTime The maximum dateTime of AnalysisResults to look at
    */
  def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.before = Option(dateTime)
    this
  }

  /**
    * Only look at AnalysisResults with a history key with a greater value
    *
    * @param dateTime The minimum dateTime of AnalysisResults to look at
    */
  def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.after = Option(dateTime)
    this
  }

  /** Get the AnalysisResult */
  def get(): Seq[StatisticsResult] = {
    resultsRepository
      .filterKeys(key => after.isEmpty || after.get <= key.dataSetDate)
      .filterKeys(key => before.isEmpty || key.dataSetDate <= before.get)
      .filterKeys(key => tagValues.isEmpty || tagValues.get.toSet.subsetOf(key.tags.toSet))
      .values
      .map { analysisResult =>

        val requestedMetrics = analysisResult
          .computedStatistics
          .metricMap
          .filterKeys(analyzer => forAnalyzers.isEmpty || forAnalyzers.get.contains(analyzer))

        StatisticsResult(analysisResult.resultKey, ComputedStatistics(requestedMetrics))
      }
      .toSeq
  }
}
