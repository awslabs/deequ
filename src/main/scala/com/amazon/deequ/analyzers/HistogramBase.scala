/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.deequ.analyzers.Histogram.AggregateFunction
import com.amazon.deequ.analyzers.runners.{IllegalAnalyzerParameterException, MetricCalculationException}
import com.amazon.deequ.metrics.{Distribution, DistributionValue, HistogramMetric}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.ListMap
import scala.util.{Failure, Try}

/**
 * Base class for histogram analyzers that provides shared functionality.
 */
abstract class HistogramBase(
                              val column: String,
                              val binningUdf: Option[UserDefinedFunction],
                              val maxDetailBins: Integer,
                              val where: Option[String],
                              val computeFrequenciesAsRatio: Boolean,
                              val aggregateFunction: AggregateFunction)
  extends Analyzer[FrequenciesAndNumRows, HistogramMetric]
    with FilterableAnalyzer {

  protected[this] val PARAM_CHECK: StructType => Unit = { _ =>
    if (maxDetailBins > Histogram.MaximumAllowedDetailBins) {
      throw new IllegalAnalyzerParameterException(s"Cannot return histogram values for more " +
        s"than ${Histogram.MaximumAllowedDetailBins} values")
    }
  }

  override def filterCondition: Option[String] = where

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

          // sort in descending frequency
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
            }(collection.breakOut): ListMap[String, DistributionValue]

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

  protected def filterOptional(where: Option[String])(data: DataFrame): DataFrame = {
    where match {
      case Some(condition) => data.filter(condition)
      case _ => data
    }
  }

  protected def binOptional(binningUdf: Option[UserDefinedFunction])(data: DataFrame): DataFrame = {
    binningUdf match {
      case Some(bin) => data.withColumn(column, bin(col(column)))
      case _ => data
    }
  }

  protected def query(data: DataFrame): DataFrame = {
    aggregateFunction.query(this.column, data)
  }
}
