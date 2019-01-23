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

import java.sql.Connection

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{JdbcAnalysis, State}
import com.amazon.deequ.metrics.{DoubleMetric, Metric}
import com.amazon.deequ.repository.{JdbcMetricsRepository, ResultKey}
import org.apache.spark.storage.StorageLevel

import scala.util.Success

private[deequ] case class JdbcAnalysisRunnerRepositoryOptions(
      metricsRepository: Option[JdbcMetricsRepository] = None,
      reuseExistingResultsForKey: Option[ResultKey] = None,
      failIfResultsForReusingMissing: Boolean = false,
      saveOrAppendResultsWithKey: Option[ResultKey] = None)

private[deequ] case class JdbcAnalysisRunnerFileOutputOptions(
      connection: Option[Connection] = None,
      saveSuccessMetricsJsonToPath: Option[String] = None,
      overwriteOutputFiles: Boolean = false)

/**
  * Runs a set of analyzers on the data at hand and optimizes the resulting computations to minimize
  * the number of scans over the data. Additionally, the internal states of the computation can be
  * stored and aggregated with existing states to enable incremental computations.
  */
object JdbcAnalysisRunner {

   /**
    * Starting point to construct an AnalysisRun.
    *
    * @param data tabular data on which the checks should be verified
    */
  def onData(table: Table): JdbcAnalysisRunBuilder = {
    new JdbcAnalysisRunBuilder(table)
  }

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
      table: Table,
      analysis: JdbcAnalysis,
      aggregateWith: Option[JdbcStateLoader] = None,
      saveStatesWith: Option[JdbcStatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)
    : JdbcAnalyzerContext = {

    doAnalysisRun(table, analysis.analyzers, aggregateWith, saveStatesWith,
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
      table: Table,
      analyzers: Seq[JdbcAnalyzer[_, Metric[_]]],
      aggregateWith: Option[JdbcStateLoader] = None,
      saveStatesWith: Option[JdbcStatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      metricsRepositoryOptions: JdbcAnalysisRunnerRepositoryOptions =
      JdbcAnalysisRunnerRepositoryOptions(),
      fileOutputOptions: JdbcAnalysisRunnerFileOutputOptions =
      JdbcAnalysisRunnerFileOutputOptions())
    : JdbcAnalyzerContext = {

    if (analyzers.isEmpty) {
      return JdbcAnalyzerContext.empty
    }

    val allAnalyzers = analyzers.map { _.asInstanceOf[JdbcAnalyzer[State[_], Metric[_]]] }

    /* We do not want to recalculate calculated metrics in the MetricsRepository */
    val resultsComputedPreviously: JdbcAnalyzerContext =
      (metricsRepositoryOptions.metricsRepository,
        metricsRepositoryOptions.reuseExistingResultsForKey)
        match {
          case (Some(metricsRepository: JdbcMetricsRepository), Some(resultKey: ResultKey)) =>
            metricsRepository.loadByKey(resultKey).getOrElse(JdbcAnalyzerContext.empty)
          case _ => JdbcAnalyzerContext.empty
        }

    val analyzersAlreadyRan = resultsComputedPreviously.metricMap.keys.toSet

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
        Preconditions.findFirstFailing(table, analyzer.preconditions).isEmpty
      }

    val failedAnalyzers = analyzersToRun.diff(passedAnalyzers)

    /* Create the failure metrics from the precondition violations */
    val preconditionFailures = computePreconditionFailureMetrics(failedAnalyzers, table)

    /* Identify analyzers which require us to group the data */
    val (groupingAnalyzers, scanningAnalyzers) =
      passedAnalyzers.partition { _.isInstanceOf[JdbcGroupingAnalyzer[State[_], Metric[_]]] }

    /* Run the analyzers which do not require grouping in a single pass over the data */
    val nonGroupedMetrics =
      runScanningAnalyzers(table, scanningAnalyzers, aggregateWith, saveStatesWith)

    // TODO this can be further improved, we can get the number of rows from other metrics as well
    // TODO we could also insert an extra JdbcSize() computation if we have to scan the data anyways
    var numRowsOfData = nonGroupedMetrics.metric(JdbcSize()).collect {
      case DoubleMetric(_, _, _, Success(value: Double)) => value.toLong
    }

    var groupedMetrics = JdbcAnalyzerContext.empty

    /* Run grouping analyzers based on the columns which they need to group on */
    groupingAnalyzers
      .map { _.asInstanceOf[JdbcGroupingAnalyzer[State[_], Metric[_]]] }
      .groupBy { _.groupingColumns().sorted }
      .foreach { case (groupingColumns, analyzersForGrouping) =>

        val (numRows, metrics) =
          runGroupingAnalyzers(table, groupingColumns, analyzersForGrouping, aggregateWith,
            saveStatesWith, storageLevelOfGroupedDataForMultiplePasses, numRowsOfData)

        groupedMetrics = groupedMetrics ++ metrics

        /* if we don't know the size of the data yet, we know it after the first pass */
        if (numRowsOfData.isEmpty) {
          numRowsOfData = Option(numRows)
        }
      }

    val resultingAnalyzerContext = resultsComputedPreviously ++ preconditionFailures ++
      nonGroupedMetrics ++ groupedMetrics

    saveOrAppendResultsIfNecessary(
      resultingAnalyzerContext,
      metricsRepositoryOptions.metricsRepository,
      metricsRepositoryOptions.saveOrAppendResultsWithKey)

    saveJsonOutputsToFilesystemIfNecessary(fileOutputOptions, resultingAnalyzerContext)

    resultingAnalyzerContext
  }

  private[this]
  def saveOrAppendResultsIfNecessary(
                                      resultingAnalyzerContext: JdbcAnalyzerContext,
                                      metricsRepository: Option[JdbcMetricsRepository],
                                      saveOrAppendResultsWithKey: Option[ResultKey])
    : Unit = {

    metricsRepository.foreach { repository =>
      saveOrAppendResultsWithKey.foreach { key =>

        val currentValueForKey = repository.loadByKey(key).getOrElse(JdbcAnalyzerContext.empty)

        // AnalyzerContext entries on the right side of ++ will overwrite the ones on the left
        // if there are two different metric results for the same analyzer
        val valueToSave = currentValueForKey ++ resultingAnalyzerContext

        repository.save(saveOrAppendResultsWithKey.get, valueToSave)
      }
    }
  }

  private[this] def saveJsonOutputsToFilesystemIfNecessary(
    fileOutputOptions: JdbcAnalysisRunnerFileOutputOptions,
    analyzerContext: JdbcAnalyzerContext)
  : Unit = {

    fileOutputOptions.connection.foreach { connection =>
      fileOutputOptions.saveSuccessMetricsJsonToPath.foreach { profilesOutput =>

        // TODO: save to local FS
        /*
        DfsUtils.writeToTextFileOnDfs(connection, profilesOutput,
          overwrite = fileOutputOptions.overwriteOutputFiles) { writer =>
            writer.append(JdbcAnalyzerContext.successMetricsAsJson(analyzerContext))
            writer.newLine()
          }
          */
        }
    }
  }

  private[this] def computePreconditionFailureMetrics(
      failedAnalyzers: Seq[JdbcAnalyzer[State[_], Metric[_]]],
      table: Table)
    : JdbcAnalyzerContext = {

    val failures = failedAnalyzers.map { analyzer =>

      val firstException = Preconditions
        .findFirstFailing(table, analyzer.preconditions).get

      analyzer -> analyzer.toFailureMetric(firstException)
    }
    .toMap[JdbcAnalyzer[_, Metric[_]], Metric[_]]

    JdbcAnalyzerContext(failures)
  }


  private[this] def runGroupingAnalyzers(
      table: Table,
      groupingColumns: Seq[String],
      analyzers: Seq[JdbcGroupingAnalyzer[State[_], Metric[_]]],
      aggregateWith: Option[JdbcStateLoader],
      saveStatesTo: Option[JdbcStatePersister],
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel,
      numRowsOfData: Option[Long])
    : (Long, JdbcAnalyzerContext) = {

    /* Compute the frequencies of the request groups once */
    var frequenciesAndNumRows =
      JdbcFrequencyBasedAnalyzer.computeFrequencies(table, groupingColumns)

    /* Pick one analyzer to store the state for */
    val sampleAnalyzer =
      analyzers.head.asInstanceOf[JdbcAnalyzer[JdbcFrequenciesAndNumRows, Metric[_]]]

    /* Potentially aggregate states */
    aggregateWith
      .foreach { _.load[JdbcFrequenciesAndNumRows](sampleAnalyzer)
        .foreach { previousFrequenciesAndNumRows =>
          frequenciesAndNumRows = frequenciesAndNumRows.sum(previousFrequenciesAndNumRows)
        }
      }

    val results = runAnalyzersForParticularGrouping(frequenciesAndNumRows, analyzers, saveStatesTo,
        storageLevelOfGroupedDataForMultiplePasses)

    frequenciesAndNumRows.numRows -> results
  }

  private[this] def runScanningAnalyzers(
      table: Table,
      analyzers: Seq[JdbcAnalyzer[State[_], Metric[_]]],
      aggregateWith: Option[JdbcStateLoader] = None,
      saveStatesTo: Option[JdbcStatePersister] = None)
    : JdbcAnalyzerContext = {

    /* Identify shareable analyzers */
    val (shareable, others) =
      analyzers.partition { _.isInstanceOf[JdbcScanShareableAnalyzer[_, _]] }

    val shareableAnalyzers =
      shareable.map { _.asInstanceOf[JdbcScanShareableAnalyzer[State[_], Metric[_]]] }

    /* Compute aggregation functions of shareable analyzers in a single pass over the data */
    val sharedResults = if (shareableAnalyzers.nonEmpty) {
      val metricsByAnalyzer = try {
        val aggregations = shareableAnalyzers.flatMap { _.aggregationFunctions() }

        /* Compute offsets so that the analyzers can correctly pick their results from the row */
        val offsets = shareableAnalyzers.scanLeft(0) { case (current, analyzer) =>
          current + analyzer.aggregationFunctions().length
        }

        val results = table.executeAggregations(aggregations)

        shareableAnalyzers.zip(offsets).map { case (analyzer, offset) =>
          analyzer ->
            successOrFailureMetricFrom(analyzer, results, offset, aggregateWith, saveStatesTo)
        }

      } catch {
        case error: Exception =>
          shareableAnalyzers.map { analyzer => analyzer -> analyzer.toFailureMetric(error) }
      }

      JdbcAnalyzerContext(metricsByAnalyzer.toMap[JdbcAnalyzer[_, Metric[_]], Metric[_]])
    } else {
      JdbcAnalyzerContext.empty
    }

    /* Run non-shareable analyzers separately */
    val otherMetrics = others
      .map { analyzer => analyzer -> analyzer.calculate(table) }
      .toMap[JdbcAnalyzer[_, Metric[_]], Metric[_]]

    sharedResults ++ JdbcAnalyzerContext(otherMetrics)
  }

  /** Compute scan-shareable analyzer metric from aggregation result, mapping generic exceptions
    * to a failure metric */
  private def successOrFailureMetricFrom(
      analyzer: JdbcScanShareableAnalyzer[State[_], Metric[_]],
      aggregationResult: JdbcRow,
      offset: Int,
      aggregateWith: Option[JdbcStateLoader] = None,
      saveStatesTo: Option[JdbcStatePersister] = None)
    : Metric[_] = {

    try {
      analyzer.metricFromAggregationResult(aggregationResult, offset, aggregateWith, saveStatesTo)
    } catch {
      case error: Exception => analyzer.toFailureMetric(error)
    }
  }

  /**
    * Compute frequency based analyzer metric from aggregation result, mapping generic exceptions
    * to a failure metric */
  private def successOrFailureMetricFrom(
      analyzer: JdbcScanShareableFrequencyBasedAnalyzer,
      aggregationResult: JdbcRow,
      offset: Int)
    : Metric[_] = {

    try {
      analyzer.fromAggregationResult(aggregationResult, offset)
    } catch {
      case error: Exception => analyzer.toFailureMetric(error)
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
      table: Table,
      analysis: JdbcAnalysis,
      stateLoaders: Seq[JdbcStateLoader],
      saveStatesWith: Option[JdbcStatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      metricsRepository: Option[JdbcMetricsRepository] = None,
      saveOrAppendResultsWithKey: Option[ResultKey] = None                             )
    : JdbcAnalyzerContext = {

    if (analysis.analyzers.isEmpty || stateLoaders.isEmpty) {
      return JdbcAnalyzerContext.empty
    }

    val analyzers = analysis.analyzers.map { _.asInstanceOf[JdbcAnalyzer[State[_], Metric[_]]] }

    /* Find all analyzers which violate their preconditions */
    val passedAnalyzers = analyzers
      .filter { analyzer =>
        Preconditions.findFirstFailing(table, analyzer.preconditions).isEmpty
      }

    val failedAnalyzers = analyzers.diff(passedAnalyzers)

    /* Create the failure metrics from the precondition violations */
    val preconditionFailures = computePreconditionFailureMetrics(failedAnalyzers, table)

    val aggregatedStates = JdbcInMemoryStateProvider()

    /* Aggregate all initial states */
    passedAnalyzers.foreach { analyzer =>
      stateLoaders.foreach { stateLoader =>
        analyzer.aggregateStateTo(aggregatedStates, stateLoader, aggregatedStates)
      }
    }

    /* Identify analyzers which require us to group the data */
    val (groupingAnalyzers, scanningAnalyzers) =
      passedAnalyzers.partition { _.isInstanceOf[JdbcGroupingAnalyzer[State[_], Metric[_]]] }

    val nonGroupedResults = scanningAnalyzers
      .map { _.asInstanceOf[JdbcAnalyzer[State[_], Metric[_]]] }
      .flatMap { analyzer =>
        analyzer
          .loadStateAndComputeMetric(aggregatedStates)
          .map { metric => analyzer -> metric }
      }
      .toMap[JdbcAnalyzer[_, Metric[_]], Metric[_]]


    val groupedResults = if (groupingAnalyzers.isEmpty) {
      JdbcAnalyzerContext.empty
    } else {
      groupingAnalyzers
        .map { _.asInstanceOf[JdbcGroupingAnalyzer[State[_], Metric[_]]] }
        .groupBy { _.groupingColumns().sorted }
        .map { case (_, analyzersForGrouping) =>

          val state = findStateForParticularGrouping(analyzersForGrouping, aggregatedStates)

          runAnalyzersForParticularGrouping(state, analyzersForGrouping, saveStatesWith,
            storageLevelOfGroupedDataForMultiplePasses)
        }
        .reduce { _ ++ _ }
    }

    val results = preconditionFailures ++ JdbcAnalyzerContext(nonGroupedResults) ++ groupedResults

    saveOrAppendResultsIfNecessary(results, metricsRepository, saveOrAppendResultsWithKey)

    results
  }

  /** We only store the grouped dataframe for a particular grouping once; in order to retrieve it
    * for analyzers that require it, we need to test all of them */
  private[this] def findStateForParticularGrouping(
      analyzers: Seq[JdbcGroupingAnalyzer[State[_], Metric[_]]], stateLoader: JdbcStateLoader)
    : JdbcFrequenciesAndNumRows = {

    /* One of the analyzers must have the state persisted */
    val states = analyzers.flatMap { analyzer =>
      stateLoader
        .load[JdbcFrequenciesAndNumRows](
          analyzer.asInstanceOf[JdbcAnalyzer[JdbcFrequenciesAndNumRows, _]])
    }

    require(states.nonEmpty)
    states.head
  }

  /** Efficiently executes the analyzers for a particular grouping,
    * applying scan-sharing where possible */
  private[this] def runAnalyzersForParticularGrouping(
      frequenciesAndNumRows: JdbcFrequenciesAndNumRows,
      analyzers: Seq[JdbcGroupingAnalyzer[State[_], Metric[_]]],
      saveStatesTo: Option[JdbcStatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)
    : JdbcAnalyzerContext = {

    val numRows = frequenciesAndNumRows.numRows

    /* Identify all shareable analyzers */
    val (shareable, others) =
      analyzers.partition { _.isInstanceOf[JdbcScanShareableFrequencyBasedAnalyzer] }

    /* Potentially cache the grouped data if we need to make several passes,
       controllable via the storage level */
    if (others.nonEmpty) {
      // TODO
      // frequenciesAndNumRows.frequencies().persist(storageLevelOfGroupedDataForMultiplePasses)
    }

    val shareableAnalyzers = shareable.map {
      _.asInstanceOf[JdbcScanShareableFrequencyBasedAnalyzer] }

    val metricsByAnalyzer = if (shareableAnalyzers.nonEmpty) {

      try {
        val aggregations = shareableAnalyzers.flatMap { _.aggregationFunctions(numRows) }
        /* Compute offsets so that the analyzers can correctly pick their results from the row */
        val offsets = shareableAnalyzers.scanLeft(0) { case (current, analyzer) =>
          current + analyzer.aggregationFunctions(numRows).length
        }

        /* Execute aggregation on grouped data */
        val results = frequenciesAndNumRows.table.executeAggregations(aggregations)

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
        .map { _.asInstanceOf[JdbcFrequencyBasedAnalyzer] }
        .map { analyzer => analyzer ->
          analyzer.computeMetricFrom(Option(frequenciesAndNumRows))
        }
    } catch {
      case error: Exception =>
        others.map { analyzer => analyzer -> analyzer.toFailureMetric(error) }
    }

    /* Potentially store states */
    saveStatesTo.foreach { _.persist(analyzers.head, frequenciesAndNumRows) }

    // TODO
    // frequenciesAndNumRows.frequencies.unpersist()

    JdbcAnalyzerContext(
      (metricsByAnalyzer ++ otherMetrics).toMap[JdbcAnalyzer[_, Metric[_]], Metric[_]])
  }
}
