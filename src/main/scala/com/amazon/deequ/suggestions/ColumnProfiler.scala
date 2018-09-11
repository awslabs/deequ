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

package com.amazon.deequ.suggestions

import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, ApproxQuantiles, Completeness, DataType, DataTypeHistogram, DataTypeInstances, Histogram, Maximum, Mean, Minimum, Size, StandardDeviation}
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.{Distribution, DistributionValue, DoubleMetric, HistogramMetric, KeyedDoubleMetric}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType, DataType => SparkDataType}
import DataTypeInstances._

import scala.util.Success

private[deequ] case class GenericColumnStatistics(
    numRecords: Long,
    inferredTypes: Map[String, DataTypeInstances.Value],
    knownTypes: Map[String, DataTypeInstances.Value],
    typeDetectionHistograms: Map[String, Map[String, Long]],
    approximateNumDistincts: Map[String, Long],
    completenesses: Map[String, Double]) {

  def typeOf(column: String): DataTypeInstances.Value = {
    val inferredAndKnown = inferredTypes ++ knownTypes
    inferredAndKnown(column)
  }
}

private[deequ] case class NumericColumnStatistics(
    means: Map[String, Double],
    stdDevs: Map[String, Double],
    minima: Map[String, Double],
    maxima: Map[String, Double],
    approxPercentiles: Map[String, Array[Double]]
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
    * Profile a (potentially very large) dataset
    *
    * @param data dataset as dataframe
    * @param columns  the columns in the data which we want to profile
    * @param lowCardinalityHistogramThreshold the maximum  (estimated) number of distinct values
    *                                         in a column until which we should compute exact
    *                                         histograms for it (defaults to 120)
    * @return
    */
  def profile(
      data: DataFrame,
      columns: Seq[String],
      lowCardinalityHistogramThreshold: Int = DEFAULT_CARDINALITY_THRESHOLD)
    : ColumnProfiles = {

    // Ensure that all desired columns exist
    columns.foreach { columnName =>
      require(data.schema.fieldNames.contains(columnName), s"Unable to find column $columnName")
    }

    println("### PROFILING: Computing generic column statistics in pass (1/3)...")

    // We compute completeness, approximate number of distinct values
    // and type detection for string columns in the first pass
    val analyzersForGenericStats = data.schema.fields
      .filter { field => columns.contains(field.name) }
      .flatMap { field =>

        val name = field.name

        if (field.dataType == StringType) {
          Seq(Completeness(name), ApproxCountDistinct(name), DataType(name))
        } else {
          Seq(Completeness(name), ApproxCountDistinct(name))
        }
      }

    val firstPassResults = Analysis(analyzersForGenericStats :+ Size()).run(data)

    val genericStatistics = extractGenericStatistics(columns, data.schema, firstPassResults)

    println("### PROFILING: Computing numeric column statistics in pass (2/3)...")

    // We cast all string columns that were detected as numeric
    val castedDataForSecondPass = castNumericStringColumns(columns, data, genericStatistics)

    // We compute mean, stddev, min, max for all numeric columns
    val analyzersForSecondPass = columns
      .filter { name => Set(Integral, Fractional).contains(genericStatistics.typeOf(name)) }
      .flatMap { name =>

        val percentiles = (1 to 100).map { _.toDouble / 100 }

        Seq(Minimum(name), Maximum(name), Mean(name), StandardDeviation(name),
          ApproxQuantiles(name, percentiles))
      }


    val secondPassResults = Analysis(analyzersForSecondPass).run(castedDataForSecondPass)
    val numericStatistics = extractNumericStatistics(secondPassResults)


    println("### PROFILING: Computing histograms of low-cardinality columns in pass (3/3)...")

    // We compute exact histograms for all low-cardinality string columns
    val targetColumnsForHistograms = findTargetColumnsForHistograms(data.schema, genericStatistics,
      lowCardinalityHistogramThreshold)

    val histograms: Map[String, Distribution] = if (targetColumnsForHistograms.nonEmpty) {
      computeHistograms(data, targetColumnsForHistograms)
    } else {
      println("### PROFILING: Skipping pass (3/3), no target columns found.")
      Map.empty
    }

    val thirdPassResults = CategoricalColumnStatistics(histograms)

    createProfiles(columns, genericStatistics, numericStatistics, thirdPassResults)
  }

  /* Cast string columns detected as numeric to their detected type */
  private[suggestions] def castColumn(
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
      results: AnalyzerContext)
    : GenericColumnStatistics = {

    val numRecords = results.metricMap
      .collect { case (_: Size, metric: DoubleMetric) => metric.value.get }
      .head
      .toLong

    val inferredTypes = results.metricMap
      .collect { case (analyzer: DataType, metric: HistogramMetric) =>
        val typeHistogram = metric.value.get
        analyzer.column -> DataTypeHistogram.determineType(typeHistogram)
      }

    val typeDetectionHistograms = results.metricMap
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
      .filter { _.dataType != StringType }
      .map { field =>
        val knownType = field.dataType match {
          case ShortType | LongType | IntegerType => Integral
          case DecimalType() | FloatType | DoubleType => Fractional
          case BooleanType => Boolean
          case TimestampType => String  // TODO We should have support for dates in deequ...
          case _ =>
            println(s"Unable to map type ${field.dataType}")
            Unknown
        }

        field.name -> knownType
      }
      .toMap

    GenericColumnStatistics(numRecords, inferredTypes, knownTypes, typeDetectionHistograms,
      approximateNumDistincts, completenesses)
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

    val approxPercentiles = results.metricMap
      .collect {  case (analyzer: ApproxQuantiles, metric: KeyedDoubleMetric) =>
        metric.value match {
          case Success(metricValue) =>
            val percentiles = metricValue.values.toArray.sorted
            Some(analyzer.column -> percentiles)
          case _ => None
        }
      }
      .flatten
      .toMap

    NumericColumnStatistics(means, stdDevs, minima, maxima, approxPercentiles)
  }

  /* Identifies all string columns, which:
   *
   * (1) have not been detected as a particular type
   * (2) have less than `lowCardinalityHistogramThreshold` approximate distinct values
   */
  private[this] def findTargetColumnsForHistograms(
      schema: StructType,
      genericStatistics: GenericColumnStatistics,
      lowCardinalityHistogramThreshold: Long)
    : Seq[String] = {

    val originalStringColumns = schema
      .flatMap { field => if (field.dataType == StringType) Some(field.name) else None }
      .toSet

    genericStatistics.approximateNumDistincts
      .filter { case (column, _) => originalStringColumns.contains(column) }
      .filter { case (column, _) => genericStatistics.typeOf(column) == String }
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
            row.getString(index)
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
              numericStats.means.get(name),
              numericStats.maxima.get(name),
              numericStats.minima.get(name),
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
