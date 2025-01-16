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

package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Histogram.{AggregateFunction, Count}
import com.amazon.deequ.analyzers.runners.{IllegalAnalyzerParameterException, MetricCalculationException}
import com.amazon.deequ.metrics.{Distribution, DistributionValue, HistogramMetric}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Try}

/**
  * Histogram is the summary of values in a column of a DataFrame. Groups the given column's values,
  * and calculates either number of rows or with that specific value and the fraction of this value or
  * sum of values in other column.
  *
  * @param column        Column to do histogram analysis on
  * @param binningUdf    Optional binning function to run before grouping to re-categorize the
  *                      column values.
  *                      For example to turn a numerical value to a categorical value binning
  *                      functions might be used.
  * @param maxDetailBins Histogram details is only provided for N column values with top counts.
  *                      maxBins sets the N.
  *                      This limit does not affect what is being returned as number of bins. It
  *                      always returns the dictinct value count.
  * @param aggregateFunction function that implements aggregation logic.
  */
case class Histogram(
    column: String,
    binningUdf: Option[UserDefinedFunction] = None,
    maxDetailBins: Integer = Histogram.MaximumAllowedDetailBins,
    where: Option[String] = None,
    computeFrequenciesAsRatio: Boolean = true,
    aggregateFunction: AggregateFunction = Count)
  extends Analyzer[FrequenciesAndNumRows, HistogramMetric]
  with FilterableAnalyzer {

  private[this] val PARAM_CHECK: StructType => Unit = { _ =>
    if (maxDetailBins > Histogram.MaximumAllowedDetailBins) {
      throw new IllegalAnalyzerParameterException(s"Cannot return histogram values for more " +
        s"than ${Histogram.MaximumAllowedDetailBins} values")
    }
  }

  override def computeStateFrom(data: DataFrame,
                                filterCondition: Option[String] = None): Option[FrequenciesAndNumRows] = {

    // TODO figure out a way to pass this in if its known before hand
    val totalCount = if (computeFrequenciesAsRatio) {
      aggregateFunction.total(data)
    } else {
      1
    }

    val df = data
      .transform(filterOptional(where))
      .transform(binOptional(binningUdf))
    val frequencies = query(df)

    Some(FrequenciesAndNumRows(frequencies, totalCount))
  }

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): HistogramMetric = {

    state match {

      case Some(theState) =>
        val value: Try[Distribution] = Try {

          val countColumnName = theState.frequencies.schema.fields
            .find(field => field.dataType == LongType && field.name != column)
            .map(_.name)
            .getOrElse(throw new IllegalStateException(s"Count column not found in the frequencies DataFrame"))

          val topNRowsDF = theState.frequencies
            .orderBy(col(countColumnName).desc)
            .limit(maxDetailBins)
            .collect()

          val binCount = theState.frequencies.count()

          val columnName = theState.frequencies.columns
            .find(_ == column)
            .getOrElse(throw new IllegalStateException(s"Column $column not found"))

          val histogramDetails = topNRowsDF
            .map { row =>
              val discreteValue = row.getAs[String](columnName)
              val absolute = row.getAs[Long](countColumnName)
              val ratio = absolute.toDouble / theState.numRows
              discreteValue -> DistributionValue(absolute, ratio)
            }
            .toMap

          Distribution(histogramDetails, binCount)
        }

        HistogramMetric(column, value)

      case None =>
        HistogramMetric(column, Failure(Analyzers.emptyStateException(this)))
    }
  }

  override def toFailureMetric(exception: Exception): HistogramMetric = {
    HistogramMetric(column, Failure(MetricCalculationException.wrapIfNecessary(exception)))
  }

  override def preconditions: Seq[StructType => Unit] = {
    PARAM_CHECK :: Preconditions.hasColumn(column) :: Nil
  }

  override def filterCondition: Option[String] = where

  private def filterOptional(where: Option[String])(data: DataFrame): DataFrame = {
    where match {
      case Some(condition) => data.filter(condition)
      case _ => data
    }
  }

  private def binOptional(binningUdf: Option[UserDefinedFunction])(data: DataFrame): DataFrame = {
    binningUdf match {
      case Some(bin) => data.withColumn(column, bin(col(column)))
      case _ => data
    }
  }

  private def query(data: DataFrame): DataFrame = {
    aggregateFunction.query(this.column, data)
  }
}

object Histogram {
  val NullFieldReplacement = "NullValue"
  val MaximumAllowedDetailBins = 1000
  val count_function = "count"
  val sum_function = "sum"

  sealed trait AggregateFunction {
    def query(column: String, data: DataFrame): DataFrame

    def total(data: DataFrame): Long

    def aggregateColumn(): Option[String]

    def function(): String
  }

  case object Count extends AggregateFunction {
    override def query(column: String, data: DataFrame): DataFrame = {
      data
        .select(col(column).cast(StringType))
        .na.fill(Histogram.NullFieldReplacement)
        .groupBy(column)
        .count()
        .withColumnRenamed("count", Analyzers.COUNT_COL)
    }

    override def aggregateColumn(): Option[String] = None

    override def function(): String = count_function

    override def total(data: DataFrame): Long = {
      data.count()
    }
  }

  case class Sum(aggColumn: String) extends AggregateFunction {
    override def query(column: String, data: DataFrame): DataFrame = {
      data
        .select(col(column).cast(StringType), col(aggColumn).cast(LongType))
        .na.fill(Histogram.NullFieldReplacement)
        .groupBy(column)
        .sum(aggColumn)
        .withColumnRenamed("count", Analyzers.COUNT_COL)
    }

    override def total(data: DataFrame): Long = {
      data.groupBy().sum(aggColumn).first().getLong(0)
    }

    override def aggregateColumn(): Option[String] = {
      Some(aggColumn)
    }

    override def function(): String = sum_function
  }
}

object OrderByAbsoluteCount extends Ordering[Row] {
  override def compare(x: Row, y: Row): Int = {
    x.getLong(1).compareTo(y.getLong(1))
  }
}
