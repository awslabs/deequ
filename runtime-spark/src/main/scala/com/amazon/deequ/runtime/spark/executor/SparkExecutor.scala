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

package com.amazon.deequ.runtime.spark.executor

import com.amazon.deequ.{ComputedStatistics, RepositoryOptions}
import com.amazon.deequ.runtime.spark.operators._
import com.amazon.deequ.metrics.{DoubleMetric, Metric}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.runtime.spark._
import com.amazon.deequ.runtime.spark.operators.State
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.Success

private[deequ] case class AnalysisRunnerFileOutputOptions(
      sparkSession: Option[SparkSession] = None,
      saveSuccessMetricsJsonToPath: Option[String] = None,
      overwriteOutputFiles: Boolean = false)

/**
  * Runs a set of analyzers on the data at hand and optimizes the resulting computations to minimize
  * the number of scans over the data. Additionally, the internal states of the computation can be
  * stored and aggregated with existing states to enable incremental computations.
  */
object SparkExecutor {

  /**
    * Compute the metrics from the analyzers configured in the analyis
    *
    * @param data data on which to operate
    * @param analysis analysis defining the analyzers to run
    * @param aggregateWith load existing states for the configured analyzers and aggregate them
    *                      (optional)
    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
    * @param storageLevelOfGroupedDataForMultiplePasses caching level for grouped data that must be
    *                                                   accessed multiple times (use
    *                                                   StorageLevel.NONE to completely disable
    *                                                   caching)
    * @return AnalyzerContext holding the requested metrics per analyzer
    */
  @deprecated("Use onData instead for a fluent API", "10-07-2019")
  def run(
      data: DataFrame,
      analysis: OperatorList,
      aggregateWith: Option[SparkStateLoader] = None,
      saveStatesWith: Option[SparkStatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)
    : OperatorResults = {

    doAnalysisRun(data, analysis.analyzers, aggregateWith, saveStatesWith,
      storageLevelOfGroupedDataForMultiplePasses)
  }

  /**
    * Compute the metrics from the analyzers configured in the analyis
    *
    * @param data data on which to operate
    * @param analyzers the analyzers to run
    * @param aggregateWith load existing states for the configured analyzers and aggregate them
    *                      (optional)
    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
    * @param storageLevelOfGroupedDataForMultiplePasses caching level for grouped data that must be
    *                                                   accessed multiple times (use
    *                                                   StorageLevel.NONE to completely disable
    *                                                   caching)
    * @param metricsRepositoryOptions Options related to the MetricsRepository
    * @param fileOutputOptions Options related to FileOuput using a SparkSession
    * @return AnalyzerContext holding the requested metrics per analyzer
    */
  private[deequ] def doAnalysisRun(
      data: DataFrame,
      analyzers: Seq[Operator[_, Metric[_]]],
      aggregateWith: Option[SparkStateLoader] = None,
      saveStatesWith: Option[SparkStatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      metricsRepositoryOptions: RepositoryOptions = RepositoryOptions(),
      fileOutputOptions: AnalysisRunnerFileOutputOptions = AnalysisRunnerFileOutputOptions())
    : OperatorResults = {

    if (analyzers.isEmpty) {
      return OperatorResults.empty
    }

    val allAnalyzers = analyzers.map { _.asInstanceOf[Operator[State[_], Metric[_]]] }

    /* We do not want to recalculate calculated metrics in the MetricsRepository */
    val resultsComputedPreviously: ComputedStatistics =
      (metricsRepositoryOptions.metricsRepository,
        metricsRepositoryOptions.reuseExistingResultsForKey)
        match {
          case (Some(metricsRepository: MetricsRepository), Some(resultKey: ResultKey)) =>
            metricsRepository.loadByKey(resultKey).getOrElse(ComputedStatistics.empty)
          case _ => ComputedStatistics.empty
        }

    val analyzersAlreadyRan = resultsComputedPreviously.metricMap
      .keys.toSet
      .map { SparkEngine.matchingOperator }

    val analyzersToRun = allAnalyzers.filterNot(analyzersAlreadyRan.contains)

    /* Throw an error if all needed metrics should have gotten calculated before but did not */
    if (metricsRepositoryOptions.failIfResultsForReusingMissing && analyzersToRun.nonEmpty) {
      throw new ReusingNotPossibleResultsMissingException(
        "Could not find all necessary results in the MetricsRepository, the calculation of " +
          s"the metrics for these analyzers would be needed: ${analyzersToRun.mkString(", ")}")
    }

    /* Find all analyzers which violate their preconditions */
    val passedAnalyzers = analyzersToRun
      .filter { analyzer =>
        Preconditions.findFirstFailing(data.schema, analyzer.preconditions).isEmpty
      }

    val failedAnalyzers = analyzersToRun.diff(passedAnalyzers)

    /* Create the failure metrics from the precondition violations */
    val preconditionFailures = computePreconditionFailureMetrics(failedAnalyzers, data.schema)

    /* Identify analyzers which require us to group the data */
    val (groupingAnalyzers, scanningAnalyzers) =
      passedAnalyzers.partition { _.isInstanceOf[GroupingOperator[State[_], Metric[_]]] }

    /* Run the analyzers which do not require grouping in a single pass over the data */
    val nonGroupedMetrics =
      runScanningAnalyzers(data, scanningAnalyzers, aggregateWith, saveStatesWith)

    // TODO this can be further improved, we can get the number of rows from other metrics as well
    // TODO we could also insert an extra Size() computation if we have to scan the data anyways
    var numRowsOfData = nonGroupedMetrics.metric(SizeOp()).collect {
      case DoubleMetric(_, _, _, Success(value: Double)) => value.toLong
    }

    var groupedMetrics = OperatorResults.empty

    /* Run grouping analyzers based on the columns which they need to group on */
    groupingAnalyzers
      .map { _.asInstanceOf[GroupingOperator[State[_], Metric[_]]] }
      .groupBy { _.groupingColumns().sorted }
      .foreach { case (groupingColumns, analyzersForGrouping) =>

        val (numRows, metrics) =
          runGroupingAnalyzers(data, groupingColumns, analyzersForGrouping, aggregateWith,
            saveStatesWith, storageLevelOfGroupedDataForMultiplePasses, numRowsOfData)

        groupedMetrics = groupedMetrics ++ metrics

        /* if we don't know the size of the data yet, we know it after the first pass */
        if (numRowsOfData.isEmpty) {
          numRowsOfData = Option(numRows)
        }
      }

    val operatorsComputedPreviously = SparkEngine.computedStatisticsToAnalyzerContext(resultsComputedPreviously)

    val resultingAnalyzerContext = operatorsComputedPreviously ++ preconditionFailures ++
      nonGroupedMetrics ++ groupedMetrics

    saveOrAppendResultsIfNecessary(
      resultingAnalyzerContext,
      metricsRepositoryOptions.metricsRepository,
      metricsRepositoryOptions.saveOrAppendResultsWithKey)

    saveJsonOutputsToFilesystemIfNecessary(fileOutputOptions, resultingAnalyzerContext)

    resultingAnalyzerContext
  }

  private[this] def saveOrAppendResultsIfNecessary(
      resultingAnalyzerContext: OperatorResults,
      metricsRepository: Option[MetricsRepository],
      saveOrAppendResultsWithKey: Option[ResultKey])
    : Unit = {

    metricsRepository.foreach { repository =>
      saveOrAppendResultsWithKey.foreach { key =>

        val currentValueForKey = repository.loadByKey(key).getOrElse(ComputedStatistics.empty)

        // AnalyzerContext entries on the right side of ++ will overwrite the ones on the left
        // if there are two different metric results for the same analyzer
        val valueToSave = currentValueForKey ++
          SparkEngine.analyzerContextToComputedStatistics(resultingAnalyzerContext)

        repository.save(saveOrAppendResultsWithKey.get, valueToSave)
      }
    }
  }

  private[this] def saveJsonOutputsToFilesystemIfNecessary(
    fileOutputOptions: AnalysisRunnerFileOutputOptions,
    analyzerContext: OperatorResults)
  : Unit = {

//FIXLATER
//    fileOutputOptions.sparkSession.foreach { session =>
//      fileOutputOptions.saveSuccessMetricsJsonToPath.foreach { profilesOutput =>
//
//        DfsUtils.writeToTextFileOnDfs(session, profilesOutput,
//          overwrite = fileOutputOptions.overwriteOutputFiles) { writer =>
//            writer.append(AnalyzerContext.successMetricsAsJson(analyzerContext))
//            writer.newLine()
//          }
//        }
//    }
  }

  private[this] def computePreconditionFailureMetrics(
      failedAnalyzers: Seq[Operator[State[_], Metric[_]]],
      schema: StructType)
    : OperatorResults = {

    val failures = failedAnalyzers.map { analyzer =>

      val firstException = Preconditions
        .findFirstFailing(schema, analyzer.preconditions).get

      analyzer -> analyzer.toFailureMetric(firstException)
    }
    .toMap[Operator[_, Metric[_]], Metric[_]]

    OperatorResults(failures)
  }

  private[this] def runGroupingAnalyzers(
      data: DataFrame,
      groupingColumns: Seq[String],
      analyzers: Seq[GroupingOperator[State[_], Metric[_]]],
      aggregateWith: Option[SparkStateLoader],
      saveStatesTo: Option[SparkStatePersister],
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel,
      numRowsOfData: Option[Long])
    : (Long, OperatorResults) = {

    /* Compute the frequencies of the request groups once */
    var frequenciesAndNumRows = FrequencyBasedOperator.computeFrequencies(data, groupingColumns)

    /* Pick one analyzer to store the state for */
    val sampleAnalyzer = analyzers.head.asInstanceOf[Operator[FrequenciesAndNumRows, Metric[_]]]

    /* Potentially aggregate states */
    aggregateWith
      .foreach { _.load[FrequenciesAndNumRows](sampleAnalyzer)
        .foreach { previousFrequenciesAndNumRows =>
          frequenciesAndNumRows = frequenciesAndNumRows.sum(previousFrequenciesAndNumRows)
        }
      }

    val results = runAnalyzersForParticularGrouping(frequenciesAndNumRows, analyzers, saveStatesTo,
        storageLevelOfGroupedDataForMultiplePasses)

    frequenciesAndNumRows.numRows -> results
  }

  private[this] def runScanningAnalyzers(
      data: DataFrame,
      analyzers: Seq[Operator[State[_], Metric[_]]],
      aggregateWith: Option[SparkStateLoader] = None,
      saveStatesTo: Option[SparkStatePersister] = None)
    : OperatorResults = {

    /* Identify shareable analyzers */
    val (shareable, others) = analyzers.partition { _.isInstanceOf[ScanShareableOperator[_, _]] }

    val shareableAnalyzers =
      shareable.map { _.asInstanceOf[ScanShareableOperator[State[_], Metric[_]]] }

    /* Compute aggregation functions of shareable analyzers in a single pass over the data */
    val sharedResults = if (shareableAnalyzers.nonEmpty) {

      val metricsByAnalyzer = try {
        val aggregations = shareableAnalyzers.flatMap { _.aggregationFunctions() }

        /* Compute offsets so that the analyzers can correctly pick their results from the row */
        val offsets = shareableAnalyzers.scanLeft(0) { case (current, analyzer) =>
          current + analyzer.aggregationFunctions().length
        }

        val results = data.agg(aggregations.head, aggregations.tail: _*).collect().head

        shareableAnalyzers.zip(offsets).map { case (analyzer, offset) =>
          analyzer ->
            successOrFailureMetricFrom(analyzer, results, offset, aggregateWith, saveStatesTo)
        }

      } catch {
        case error: Exception =>
          shareableAnalyzers.map { analyzer => analyzer -> analyzer.toFailureMetric(error) }
      }

      OperatorResults(metricsByAnalyzer.toMap[Operator[_, Metric[_]], Metric[_]])
    } else {
      OperatorResults.empty
    }

    /* Run non-shareable analyzers separately */
    val otherMetrics = others
      .map { analyzer => analyzer -> analyzer.calculate(data) }
      .toMap[Operator[_, Metric[_]], Metric[_]]

    sharedResults ++ OperatorResults(otherMetrics)
  }

  /** Compute scan-shareable analyzer metric from aggregation result, mapping generic exceptions
    * to a failure metric */
  private def successOrFailureMetricFrom(
                                          analyzer: ScanShareableOperator[State[_], Metric[_]],
                                          aggregationResult: Row,
                                          offset: Int,
                                          aggregateWith: Option[SparkStateLoader],
                                          saveStatesTo: Option[SparkStatePersister])
    : Metric[_] = {

    try {
      analyzer.metricFromAggregationResult(aggregationResult, offset, aggregateWith, saveStatesTo)
    } catch {
      case error: Exception => analyzer.toFailureMetric(error)
    }
  }

  /** Compute frequency based analyzer metric from aggregation result, mapping generic exceptions
    * to a failure metric */
  private def successOrFailureMetricFrom(
      operator: ScanShareableFrequencyBasedOperator,
      aggregationResult: Row,
      offset: Int)
    : Metric[_] = {

    try {
      operator.fromAggregationResult(aggregationResult, offset)
    } catch {
      case error: Exception => operator.toFailureMetric(error)
    }
  }

  /**
    * Compute the metrics from the analyzers configured in the analyis, instead of running
    * directly on data, this computation leverages (and aggregates) existing states which have
    * previously been computed on the data.
    *
    * @param schema schema of the data frame from which the states were computed
    * @param analysis the analysis to compute
    * @param stateLoaders loaders from which we retrieve the states to aggregate
    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
    * @param storageLevelOfGroupedDataForMultiplePasses caching level for grouped data that must be
    *                                                   accessed multiple times (use
    *                                                   StorageLevel.NONE to completely disable
    *                                                   caching)
    * @return AnalyzerContext holding the requested metrics per analyzer
    */
    def runOnAggregatedStates(
                               schema: StructType,
                               analysis: OperatorList,
                               stateLoaders: Seq[SparkStateLoader],
                               saveStatesWith: Option[SparkStatePersister] = None,
                               storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                               metricsRepository: Option[MetricsRepository] = None,
                               saveOrAppendResultsWithKey: Option[ResultKey] = None                             )
    : OperatorResults = {

    if (analysis.analyzers.isEmpty || stateLoaders.isEmpty) {
      return OperatorResults.empty
    }

    val analyzers = analysis.analyzers.map { _.asInstanceOf[Operator[State[_], Metric[_]]] }

    /* Find all analyzers which violate their preconditions */
    val passedAnalyzers = analyzers
      .filter { analyzer =>
        Preconditions.findFirstFailing(schema, analyzer.preconditions).isEmpty
      }

    val failedAnalyzers = analyzers.diff(passedAnalyzers)

    /* Create the failure metrics from the precondition violations */
    val preconditionFailures = computePreconditionFailureMetrics(failedAnalyzers, schema)

    val aggregatedStates = InMemorySparkStateProvider()

    /* Aggregate all initial states */
    passedAnalyzers.foreach { analyzer =>
      stateLoaders.foreach { stateLoader =>
        analyzer.aggregateStateTo(aggregatedStates, stateLoader, aggregatedStates)
      }
    }

    /* Identify analyzers which require us to group the data */
    val (groupingAnalyzers, scanningAnalyzers) =
      passedAnalyzers.partition { _.isInstanceOf[GroupingOperator[State[_], Metric[_]]] }

    val nonGroupedResults = scanningAnalyzers
      .map { _.asInstanceOf[Operator[State[_], Metric[_]]] }
      .flatMap { analyzer =>
        analyzer
          .loadStateAndComputeMetric(aggregatedStates)
          .map { metric => analyzer -> metric }
      }
      .toMap[Operator[_, Metric[_]], Metric[_]]


    val groupedResults = if (groupingAnalyzers.isEmpty) {
      OperatorResults.empty
    } else {
      groupingAnalyzers
        .map { _.asInstanceOf[GroupingOperator[State[_], Metric[_]]] }
        .groupBy { _.groupingColumns().sorted }
        .map { case (_, analyzersForGrouping) =>

          val state = findStateForParticularGrouping(analyzersForGrouping, aggregatedStates)

          runAnalyzersForParticularGrouping(state, analyzersForGrouping, saveStatesWith,
            storageLevelOfGroupedDataForMultiplePasses)
        }
        .reduce { _ ++ _ }
    }

    val results = preconditionFailures ++ OperatorResults(nonGroupedResults) ++ groupedResults

    saveOrAppendResultsIfNecessary(results, metricsRepository, saveOrAppendResultsWithKey)

    results
  }

  /** We only store the grouped dataframe for a particular grouping once; in order to retrieve it
    * for analyzers that require it, we need to test all of them */
  private[this] def findStateForParticularGrouping(
      analyzers: Seq[GroupingOperator[State[_], Metric[_]]], stateLoader: SparkStateLoader)
    : FrequenciesAndNumRows = {

    /* One of the analyzers must have the state persisted */
    val states = analyzers.flatMap { analyzer =>
      stateLoader
        .load[FrequenciesAndNumRows](analyzer.asInstanceOf[Operator[FrequenciesAndNumRows, _]])
    }

    require(states.nonEmpty)
    states.head
  }

  /** Efficiently executes the analyzers for a particular grouping,
    * applying scan-sharing where possible */
  private[this] def runAnalyzersForParticularGrouping(
                                                       frequenciesAndNumRows: FrequenciesAndNumRows,
                                                       analyzers: Seq[GroupingOperator[State[_], Metric[_]]],
                                                       saveStatesTo: Option[SparkStatePersister] = None,
                                                       storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)
    : OperatorResults = {

    val numRows = frequenciesAndNumRows.numRows

    /* Identify all shareable analyzers */
    val (shareable, others) =
      analyzers.partition { _.isInstanceOf[ScanShareableFrequencyBasedOperator] }

    /* Potentially cache the grouped data if we need to make several passes,
       controllable via the storage level */
    if (others.nonEmpty) {
      frequenciesAndNumRows.frequencies.persist(storageLevelOfGroupedDataForMultiplePasses)
    }

    val shareableAnalyzers = shareable.map { _.asInstanceOf[ScanShareableFrequencyBasedOperator] }

    val metricsByAnalyzer = if (shareableAnalyzers.nonEmpty) {

      try {
        val aggregations = shareableAnalyzers.flatMap { _.aggregationFunctions(numRows) }
        /* Compute offsets so that the analyzers can correctly pick their results from the row */
        val offsets = shareableAnalyzers.scanLeft(0) { case (current, analyzer) =>
          current + analyzer.aggregationFunctions(numRows).length
        }

        /* Execute aggregation on grouped data */
        val results = frequenciesAndNumRows.frequencies
          .agg(aggregations.head, aggregations.tail: _*)
          .collect()
          .head

        shareableAnalyzers.zip(offsets)
          .map { case (analyzer, offset) =>
            analyzer -> successOrFailureMetricFrom(analyzer, results, offset)
          }
      } catch {
        case error: Exception =>
          shareableAnalyzers
            .map { analyzer => analyzer -> analyzer.toFailureMetric(error) }
      }

    } else {
      Map.empty
    }

    /* Execute remaining analyzers on grouped data */
    val otherMetrics = try {
      others
        .map { _.asInstanceOf[FrequencyBasedOperator] }
        .map { analyzer => analyzer ->
          analyzer.computeMetricFrom(Option(frequenciesAndNumRows))
        }
    } catch {
      case error: Exception =>
        others.map { analyzer => analyzer -> analyzer.toFailureMetric(error) }
    }

    /* Potentially store states */
    saveStatesTo.foreach { _.persist(analyzers.head, frequenciesAndNumRows) }

    frequenciesAndNumRows.frequencies.unpersist()

    OperatorResults((metricsByAnalyzer ++ otherMetrics).toMap[Operator[_, Metric[_]], Metric[_]])
  }

}

class ReusingNotPossibleResultsMissingException(message: String)
  extends RuntimeException(message)
