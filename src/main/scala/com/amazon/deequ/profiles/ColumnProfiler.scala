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

package com.amazon.deequ.profiles

import scala.util.Success

import com.amazon.deequ.analyzers.DataTypeInstances._
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.{AnalysisRunBuilder, AnalysisRunner, AnalyzerContext, ReusingNotPossibleResultsMissingException}
import com.amazon.deequ.metrics._
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType, DataType => SparkDataType}

private[deequ] case class GenericColumnStatistics(
    numRecords: Long,
    inferredTypes: Map[String, DataTypeInstances.Value],
    knownTypes: Map[String, DataTypeInstances.Value],
    typeDetectionHistograms: Map[String, Map[String, Long]],
    approximateNumDistincts: Map[String, Long],
    completenesses: Map[String, Double],
    predefinedTypes: Map[String, DataTypeInstances.Value]) {

  def typeOf(column: String): DataTypeInstances.Value = {
    val inferredAndKnown = inferredTypes ++ knownTypes ++ predefinedTypes
    inferredAndKnown(column)
  }
}

private[deequ] case class NumericColumnStatistics(
    means: Map[String, Double],
    stdDevs: Map[String, Double],
    minima: Map[String, Double],
    maxima: Map[String, Double],
    sums: Map[String, Double],
    kll: Map[String, BucketDistribution],
    approxPercentiles: Map[String, Seq[Double]]
)

private[deequ] case class CategoricalColumnStatistics(histograms: Map[String, Distribution])

/** Computes single-column profiles in three scans over the data, intented for large (TB) datasets
  *
  * In the first phase, we compute the number of records, as well as the datatype, approx. num
  * distinct values and the completeness of each column in the sample.
  *
  * In the second phase, we compute min, max and mean for numeric columns (in the future, we should
  * add quantiles once they become scan-shareable)
  *
  * In the third phase, we compute histograms for all columns with less than
  * `lowCardinalityHistogramThreshold` (approx.) distinct values
  *
  */
object ColumnProfiler {

  val DEFAULT_CARDINALITY_THRESHOLD = 120

  /**
   * Profile a (potentially very large) dataset.
   *
   * @param data                             data dataset as dataframe
   * @param restrictToColumns                an contain a subset of columns to profile, otherwise
   *                                         all columns will be considered
   * @param printStatusUpdates
   * @param lowCardinalityHistogramThreshold the maximum (estimated) number of distinct values
   *                                         in a column until which we should compute exact
   *                                         histograms for it (defaults to 120)
   * @param metricsRepository                the repo to store metrics
   * @param reuseExistingResultsUsingKey     key for reuse existing result
   * @param failIfResultsForReusingMissing   true if we have results for reusing
   * @param saveInMetricsRepositoryUsingKey  key for saving in metrics repo
   * @param kllParameters                    parameters for KLL Sketches
   *
   * @return the profile of columns
   */
  // scalastyle:off argcount
  private[deequ] def profile(
      data: DataFrame,
      restrictToColumns: Option[Seq[String]] = None,
      printStatusUpdates: Boolean = false,
      lowCardinalityHistogramThreshold: Int =
        ColumnProfiler.DEFAULT_CARDINALITY_THRESHOLD,
      metricsRepository: Option[MetricsRepository] = None,
      reuseExistingResultsUsingKey: Option[ResultKey] = None,
      failIfResultsForReusingMissing: Boolean = false,
      saveInMetricsRepositoryUsingKey: Option[ResultKey] = None,
      kllProfiling: Boolean = false,
      kllParameters: Option[KLLParameters] = None,
      predefinedTypes: Map[String, DataTypeInstances.Value] = Map.empty)
    : ColumnProfiles = {

    // Ensure that all desired columns exist
    restrictToColumns.foreach { restrictToColumns =>
      restrictToColumns.foreach { columnName =>
        require(data.schema.fieldNames.contains(columnName), s"Unable to find column $columnName")
      }
    }

    // Find columns we want to profile
    val relevantColumns = getRelevantColumns(data.schema, restrictToColumns)

    // First pass
    if (printStatusUpdates) {
      println("### PROFILING: Computing generic column statistics in pass (1/3)...")
    }

    // We compute completeness, approximate number of distinct values
    // and type detection for string columns in the first pass
    val analyzersForGenericStats = getAnalyzersForGenericStats(
      data.schema,
      relevantColumns,
      predefinedTypes)

    var analysisRunnerFirstPass = AnalysisRunner
      .onData(data)
      .addAnalyzers(analyzersForGenericStats)
      .addAnalyzer(Size())

    analysisRunnerFirstPass = setMetricsRepositoryConfigurationIfNecessary(
      analysisRunnerFirstPass,
      metricsRepository,
      reuseExistingResultsUsingKey,
      failIfResultsForReusingMissing,
      saveInMetricsRepositoryUsingKey)

    val firstPassResults = analysisRunnerFirstPass.run()

    val genericStatistics = extractGenericStatistics(
      relevantColumns,
      data.schema,
      firstPassResults,
      predefinedTypes)

    // Second pass
    if (printStatusUpdates) {
      println("### PROFILING: Computing numeric column statistics in pass (2/3)...")
    }

    // We cast all string columns that were detected as numeric
    val castedDataForSecondPass = castNumericStringColumns(relevantColumns, data,
      genericStatistics)

    // We compute mean, stddev, min, max for all numeric columns
    val analyzersForSecondPass = getAnalyzersForSecondPass(relevantColumns,
      genericStatistics, kllProfiling, kllParameters)

    var analysisRunnerSecondPass = AnalysisRunner
      .onData(castedDataForSecondPass)
      .addAnalyzers(analyzersForSecondPass)

    analysisRunnerSecondPass = setMetricsRepositoryConfigurationIfNecessary(
      analysisRunnerSecondPass,
      metricsRepository,
      reuseExistingResultsUsingKey,
      failIfResultsForReusingMissing,
      saveInMetricsRepositoryUsingKey)

    val secondPassResults = analysisRunnerSecondPass.run()

    val numericStatistics = extractNumericStatistics(secondPassResults)

    // Third pass
    if (printStatusUpdates) {
      println("### PROFILING: Computing histograms of low-cardinality columns in pass (3/3)...")
    }

    // We compute exact histograms for all low-cardinality string columns, find those here
    val targetColumnsForHistograms = findTargetColumnsForHistograms(data.schema, genericStatistics,
      lowCardinalityHistogramThreshold)

    // Find out, if we have values for those we can reuse
    val analyzerContextExistingValues = getAnalyzerContextWithHistogramResultsForReusingIfNecessary(
      metricsRepository,
      reuseExistingResultsUsingKey,
      targetColumnsForHistograms
    )

    // The columns we need to calculate the histograms for
    val nonExistingHistogramColumns = targetColumnsForHistograms
      .filter { column => analyzerContextExistingValues.metricMap.get(Histogram(column)).isEmpty }

    // Calculate and save/append results if necessary
    val histograms: Map[String, Distribution] = getHistogramsForThirdPass(
      data,
      nonExistingHistogramColumns,
      analyzerContextExistingValues,
      printStatusUpdates,
      failIfResultsForReusingMissing,
      metricsRepository,
      saveInMetricsRepositoryUsingKey)

    val thirdPassResults = CategoricalColumnStatistics(histograms)

    createProfiles(relevantColumns, genericStatistics, numericStatistics, thirdPassResults)
  }

  private[this] def getRelevantColumns(
      schema: StructType,
      restrictToColumns: Option[Seq[String]])
    : Seq[String] = {

    schema.fields
      .filter { field => restrictToColumns.isEmpty || restrictToColumns.get.contains(field.name) }
      .map { field => field.name }
  }

  private[this] def getAnalyzersForGenericStats(
      schema: StructType,
      relevantColumns: Seq[String],
      predefinedTypes: Map[String, DataTypeInstances.Value])
    : Seq[Analyzer[_, Metric[_]]] = {

    schema.fields
      .filter { field => relevantColumns.contains(field.name) }
      .flatMap { field =>

        val name = field.name

        if (field.dataType == StringType && !predefinedTypes.contains(name)) {
          Seq(Completeness(name), ApproxCountDistinct(name), DataType(name))
        } else {
          Seq(Completeness(name), ApproxCountDistinct(name))
        }
      }
  }

   private[this] def getAnalyzersForSecondPass(
      relevantColumnNames: Seq[String],
      genericStatistics: GenericColumnStatistics,
      kllProfiling: Boolean,
      kllParameters: Option[KLLParameters] = None)
    : Seq[Analyzer[_, Metric[_]]] = {
      relevantColumnNames
        .filter { name => Set(Integral, Fractional).contains(genericStatistics.typeOf(name)) }
        .flatMap { name => getNumericColAnalyzers(name, kllProfiling, kllParameters) }
    }

  private[this] def getNumericColAnalyzers(
      column: String,
      kllProfiling: Boolean,
      kllParameters: Option[KLLParameters])
    : Seq[Analyzer[_, Metric[_]]] = {
      val mandatoryAnalyzers = Seq(Minimum(column), Maximum(column), Mean(column),
        StandardDeviation(column), Sum(column))

      val optionalAnalyzers = if (kllProfiling) {
        Seq(KLLSketch(column, kllParameters))
      } else {
        Seq.empty
      }

      mandatoryAnalyzers ++ optionalAnalyzers
  }

  private[this] def setMetricsRepositoryConfigurationIfNecessary(
      analysisRunBuilder: AnalysisRunBuilder,
      metricsRepository: Option[MetricsRepository],
      reuseExistingResultsForKey: Option[ResultKey],
      failIfResultsForReusingMissing: Boolean,
      saveInMetricsRepositoryUsingKey: Option[ResultKey])
    : AnalysisRunBuilder = {

    var analysisRunBuilderResult = analysisRunBuilder

    metricsRepository.foreach { metricsRepository =>
      var analysisRunnerWithRepository = analysisRunBuilderResult.useRepository(metricsRepository)

      reuseExistingResultsForKey.foreach { resultKey =>
        analysisRunnerWithRepository = analysisRunnerWithRepository
          .reuseExistingResultsForKey(resultKey, failIfResultsForReusingMissing)
      }

      saveInMetricsRepositoryUsingKey.foreach { resultKey =>
        analysisRunnerWithRepository = analysisRunnerWithRepository
          .saveOrAppendResult(resultKey)
      }

      analysisRunBuilderResult = analysisRunnerWithRepository
    }
    analysisRunBuilderResult
  }

  private[this] def getAnalyzerContextWithHistogramResultsForReusingIfNecessary(
      metricsRepository: Option[MetricsRepository],
      reuseExistingResultsUsingKey: Option[ResultKey],
      targetColumnsForHistograms: Seq[String])
    : AnalyzerContext = {

    var analyzerContextExistingValues = AnalyzerContext.empty

    metricsRepository.foreach { metricsRepository =>
      reuseExistingResultsUsingKey.foreach { resultKey =>

        val analyzerContextWithAllPreviousResults = metricsRepository.loadByKey(resultKey)

        analyzerContextWithAllPreviousResults.foreach { analyzerContextWithAllPreviousResults =>

          val relevantEntries = analyzerContextWithAllPreviousResults.metricMap
            .filterKeys {
              case histogram: Histogram =>
                targetColumnsForHistograms.contains(histogram.column) &&
                  Histogram(histogram.column).equals(histogram)
              case _ => false
            }
          analyzerContextExistingValues = AnalyzerContext(relevantEntries)
        }
      }
    }

    analyzerContextExistingValues
  }

  private[this] def convertColumnNamesAndDistributionToHistogramWithMetric(
    columnNamesAndDistribution: Map[String, Distribution])
  : Map[Analyzer[_, Metric[_]], Metric[_]] = {

    columnNamesAndDistribution
      .map { case (columnName, distribution) =>

        val analyzer = Histogram(columnName)
        val metric = HistogramMetric(columnName, Success(distribution))

        analyzer -> metric
      }
  }

  private[this] def saveOrAppendResultsIfNecessary(
      resultingAnalyzerContext: AnalyzerContext,
      metricsRepository: Option[MetricsRepository],
      saveOrAppendResultsWithKey: Option[ResultKey])
    : Unit = {

    metricsRepository.foreach { repository =>
      saveOrAppendResultsWithKey.foreach { key =>

        val currentValueForKey = repository.loadByKey(key).getOrElse(AnalyzerContext.empty)

        // AnalyzerContext entries on the right side of ++ will overwrite the ones on the left
        // if there are two different metric results for the same analyzer
        val valueToSave = currentValueForKey ++ resultingAnalyzerContext

        repository.save(saveOrAppendResultsWithKey.get, valueToSave)
      }
    }
  }

  /* Cast string columns detected as numeric to their detected type */
  private[profiles] def castColumn(
      data: DataFrame,
      name: String,
      toType: SparkDataType)
    : DataFrame = {

    data.withColumn(s"${name}___CASTED", data(name).cast(toType))
      .drop(name)
      .withColumnRenamed(s"${name}___CASTED", name)
  }

  private[this] def extractGenericStatistics(
      columns: Seq[String],
      schema: StructType,
      results: AnalyzerContext,
      predefinedTypes: Map[String, DataTypeInstances.Value] = Map.empty)
    : GenericColumnStatistics = {

    val numRecords = results.metricMap
      .collect { case (_: Size, metric: DoubleMetric) => metric.value.get }
      .head
      .toLong


    val inferredTypes = results.metricMap
      .filterNot{
        case (analyzer: DataType, _) => predefinedTypes.contains(analyzer.column)
        case _ => true
      }
      .collect { case (analyzer: DataType, metric: HistogramMetric) =>
          val typeHistogram = metric.value.get
          analyzer.column -> DataTypeHistogram.determineType(typeHistogram)
      }

    val typeDetectionHistograms = results.metricMap
      .filterNot{
        case (analyzer: DataType, _) => predefinedTypes.contains(analyzer.column)
        case _ => true
      }
      .collect { case (analyzer: DataType, metric: HistogramMetric) =>
          val typeCounts = metric.value.get.values
            .map { case (key, distValue) => key -> distValue.absolute }
          analyzer.column -> typeCounts
      }

    val approximateNumDistincts = results.metricMap
      .collect { case (analyzer: ApproxCountDistinct, metric: DoubleMetric) =>
        analyzer.column -> metric.value.get.toLong
      }

    val completenesses = results.metricMap
      .collect { case (analyzer: Completeness, metric: DoubleMetric) =>
        analyzer.column -> metric.value.get
      }

    val knownTypes = schema.fields
      .filter { column => columns.contains(column.name) }
      .filterNot { column => predefinedTypes.contains(column.name)}
      .filter {
        _.dataType != StringType
      }
      .map { field =>
        val knownType = field.dataType match {
          case ShortType | LongType | IntegerType => Integral
          case DecimalType() | FloatType | DoubleType => Fractional
          case BooleanType => Boolean
          case TimestampType => String // TODO We should have support for dates in deequ...
          case _ =>
            println(s"Unable to map type ${field.dataType}")
            Unknown
        }

        field.name -> knownType
      }
      .toMap

    GenericColumnStatistics(numRecords, inferredTypes, knownTypes, typeDetectionHistograms,
      approximateNumDistincts, completenesses, predefinedTypes)
  }


  private[this] def castNumericStringColumns(
      columns: Seq[String],
      originalData: DataFrame,
      genericStatistics: GenericColumnStatistics)
    : DataFrame = {

    var castedData = originalData

    columns.foreach { name =>

      castedData = genericStatistics.typeOf(name) match {
        case Integral => castColumn(castedData, name, LongType)
        case Fractional => castColumn(castedData, name, DoubleType)
        case _ => castedData
      }
    }

    castedData
  }


  private[this] def extractNumericStatistics(results: AnalyzerContext): NumericColumnStatistics = {

    val means = results.metricMap
      .collect { case (analyzer: Mean, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap

    val stdDevs = results.metricMap
      .collect { case (analyzer: StandardDeviation, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap

    val maxima = results.metricMap
      .collect { case (analyzer: Maximum, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap

    val minima = results.metricMap
      .collect { case (analyzer: Minimum, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap

    val sums = results.metricMap
      .collect { case (analyzer: Sum, metric: DoubleMetric) =>
        metric.value match {
          case Success(metricValue) => Some(analyzer.column -> metricValue)
          case _ => None
        }
      }
      .flatten
      .toMap


    val kll = results.metricMap
      .collect { case (analyzer: KLLSketch, metric: KLLMetric) if metric.value.isSuccess =>
        metric.value match {
          case Success(bucketDistribution) =>
            Some(analyzer.column -> bucketDistribution)
          case _ => None
        }
      }
      .flatten
      .toMap

    val approxPercentiles = results.metricMap
      .collect {  case (analyzer: KLLSketch, metric: KLLMetric) =>
        metric.value match {
          case Success(bucketDistribution) =>

            val percentiles = bucketDistribution.computePercentiles()

            Some(analyzer.column -> percentiles.toSeq.sorted)
          case _ => None
        }
      }
      .flatten
      .toMap


    NumericColumnStatistics(means, stdDevs, minima, maxima, sums, kll, approxPercentiles)
  }

  /* Identifies all columns, which:
   *
   * (1) have string, boolean, double, float, integer, long, or short data type
   * (2) have less than `lowCardinalityHistogramThreshold` approximate distinct values
   */
  private[this] def findTargetColumnsForHistograms(
      schema: StructType,
      genericStatistics: GenericColumnStatistics,
      lowCardinalityHistogramThreshold: Long)
    : Seq[String] = {

    val validSparkDataTypesForHistograms: Set[SparkDataType] = Set(
      StringType, BooleanType, DoubleType, FloatType, IntegerType, LongType, ShortType
    )
    val originalStringNumericOrBooleanColumns = schema
      .filter { field => validSparkDataTypesForHistograms.contains(field.dataType) }
      .map { field => field.name }
      .toSet

    genericStatistics.approximateNumDistincts
      .filter { case (column, _) =>
        originalStringNumericOrBooleanColumns.contains(column) &&
          Set(String, Boolean, Integral, Fractional).contains(genericStatistics.typeOf(column))
      }
      .filter { case (_, count) => count <= lowCardinalityHistogramThreshold }
      .map { case (column, _) => column }
      .toSeq
  }

  /* Map each the values in the target columns of each row to tuples keyed by column name and value
   * ((column_name, column_value), 1)
   * and count these in a single pass over the data. This is efficient as long as the cardinality
   * of the target columns is low.
   */
  private[this] def computeHistograms(
      data: DataFrame,
      targetColumns: Seq[String])
    : Map[String, Distribution] = {

    val namesToIndexes = data.schema.fields
      .map { _.name }
      .zipWithIndex
      .toMap

    val counts = data.rdd
      .flatMap { row =>
        targetColumns.map { column =>

          val index = namesToIndexes(column)
          val valueInColumn = if (row.isNullAt(index)) {
            Histogram.NullFieldReplacement
          } else {
            row.get(index).toString
          }

          (column -> valueInColumn, 1)
        }
      }
      .countByKey()

    // Compute the empirical distribution per column from the counts
    targetColumns.map { targetColumn =>

      val countsPerColumn = counts
        .filter { case ((column, _), _) => column == targetColumn }
        .map { case ((_, value), count) => value -> count }
        .toMap

      val sum = countsPerColumn.map { case (_, count) => count }.sum

      val values = countsPerColumn
        .map { case (value, count) => value -> DistributionValue(count, count.toDouble / sum) }

      targetColumn -> Distribution(values, numberOfBins = values.size)
    }
    .toMap
  }

  def getHistogramsForThirdPass(
      data: DataFrame,
      nonExistingHistogramColumns: Seq[String],
      analyzerContextExistingValues: AnalyzerContext,
      printStatusUpdates: Boolean,
      failIfResultsForReusingMissing: Boolean,
      metricsRepository: Option[MetricsRepository],
      saveInMetricsRepositoryUsingKey: Option[ResultKey])
    : Map[String, Distribution] = {

    if (nonExistingHistogramColumns.nonEmpty) {

      // Throw an error if all required metrics should have been calculated before but did not
      if (failIfResultsForReusingMissing) {
        throw new ReusingNotPossibleResultsMissingException(
          "Could not find all necessary results in the MetricsRepository, the calculation of " +
            s"the histograms for these columns would be required: " +
            s"${nonExistingHistogramColumns.mkString(", ")}")
      }

      val columnNamesAndDistribution = computeHistograms(data, nonExistingHistogramColumns)

      // Now merge these results with the results that we want to reuse and store them if specified

      val analyzerAndHistogramMetrics = convertColumnNamesAndDistributionToHistogramWithMetric(
        columnNamesAndDistribution)

      val analyzerContext = AnalyzerContext(analyzerAndHistogramMetrics) ++
        analyzerContextExistingValues

      saveOrAppendResultsIfNecessary(analyzerContext, metricsRepository,
        saveInMetricsRepositoryUsingKey)

      // Return overall results using the more simple Distribution format
      analyzerContext.metricMap
        .map { case (histogram: Histogram, metric: HistogramMetric) if metric.value.isSuccess =>
          histogram.column -> metric.value.get
        }
    } else {
      // We do not need to calculate new histograms
      if (printStatusUpdates) {
        println("### PROFILING: Skipping pass (3/3), no new histograms need to be calculated.")
      }
      analyzerContextExistingValues.metricMap
        .map { case (histogram: Histogram, metric: HistogramMetric) if metric.value.isSuccess =>
          histogram.column -> metric.value.get
        }
    }
  }

  private[this] def createProfiles(
      columns: Seq[String],
      genericStats: GenericColumnStatistics,
      numericStats: NumericColumnStatistics,
      categoricalStats: CategoricalColumnStatistics)
    : ColumnProfiles = {

    val profiles = columns
      .map { name =>

        val completeness = genericStats.completenesses(name)
        val approxNumDistinct = genericStats.approximateNumDistincts(name)
        val dataType = genericStats.typeOf(name)
        val isDataTypeInferred = genericStats.inferredTypes.contains(name)
        val histogram = categoricalStats.histograms.get(name)

        val typeCounts = genericStats.typeDetectionHistograms.getOrElse(name, Map.empty)

        val profile = genericStats.typeOf(name) match {

          case Integral | Fractional =>
            NumericColumnProfile(
              name,
              completeness,
              approxNumDistinct,
              dataType,
              isDataTypeInferred,
              typeCounts,
              histogram,
              numericStats.kll.get(name),
              numericStats.means.get(name),
              numericStats.maxima.get(name),
              numericStats.minima.get(name),
              numericStats.sums.get(name),
              numericStats.stdDevs.get(name),
              numericStats.approxPercentiles.get(name))

          case _ =>
            StandardColumnProfile(
              name,
              completeness,
              approxNumDistinct,
              dataType,
              isDataTypeInferred,
              typeCounts,
              histogram)
        }

        name -> profile
      }
      .toMap

    ColumnProfiles(profiles, genericStats.numRecords)
  }
}
