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
import com.amazon.deequ.analyzers.runners.IllegalAnalyzerParameterException
import com.amazon.deequ.analyzers.runners.MetricCalculationException
import com.amazon.deequ.metrics.BinData
import com.amazon.deequ.metrics.DistributionBinned
import com.amazon.deequ.metrics.DistributionValue
import com.amazon.deequ.metrics.HistogramBinnedMetric
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.floor
import org.apache.spark.sql.functions.least
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType

import scala.util.Failure
import scala.util.Try

/**
 * Histogram analyzer for numerical data with binning support.
 * Currently supports equal-width bins.
 */
case class HistogramBinned(
                            override val column: String,
                            binCount: Option[Int] = None,
                            customEdges: Option[Array[Double]] = None, // TODO: implement
                            override val where: Option[String] = None,
                            override val computeFrequenciesAsRatio: Boolean = true,
                            // Reuse Histogram's AggregateFunction implementations since just grouping
                            // by bin index (which becomes categorical data)
                            override val aggregateFunction: AggregateFunction = Histogram.Count)
  extends HistogramBase(column, where, computeFrequenciesAsRatio, aggregateFunction)
    with Analyzer[FrequenciesAndNumRows, HistogramBinnedMetric] {

  require(binCount.isDefined ^ customEdges.isDefined,
    "Must specify either binCount (equal-width) or customEdges (custom)")

  private var storedEdges: Array[Double] = _

  protected[this] val PARAM_CHECK: StructType => Unit = { _ =>
    binCount.foreach { count =>
      if (count > HistogramBinned.MaximumAllowedDetailBins) {
        throw new IllegalAnalyzerParameterException(s"Cannot return histogram values for more " +
          s"than ${HistogramBinned.MaximumAllowedDetailBins} bins")
      }
    }
  }

  override def computeStateFrom(data: DataFrame,
                                filterCondition: Option[String] = None): Option[FrequenciesAndNumRows] = {

    val totalCount = if (computeFrequenciesAsRatio) {
      aggregateFunction.total(data)
    } else {
      1
    }

    val filteredData = data.transform(filterOptional(where))

    // Cast to numeric type if needed
    val columnType = filteredData.schema(column).dataType
    val numericCol = columnType match {
      case _: IntegerType | _: LongType | _: FloatType | _: DoubleType => col(column)
      case _ => col(column).cast(DoubleType)
    }

    // Compute bin edges based on strategy
    storedEdges = if (customEdges.isDefined) {
      computeCustomEdges()
    } else {
      computeEqualWidthEdges(filteredData, numericCol)
    }

    // Create binned data using DataFrame operations
    val binnedData = if (storedEdges.isEmpty) {
      filteredData.withColumn(column, lit(Histogram.NullFieldReplacement))
    } else {
      val minVal = storedEdges.head
      val binWidth = (storedEdges.last - storedEdges.head) / (storedEdges.length - 1)

      filteredData.withColumn(column,
        when(numericCol.isNull, lit(Histogram.NullFieldReplacement))
          .otherwise(
            least(
              floor((numericCol - minVal) / binWidth).cast(IntegerType),
              lit(storedEdges.length - 2)
            ).cast(StringType)
          )
      )
    }

    val frequencies = aggregateFunction.query(column, binnedData)

    Some(FrequenciesAndNumRows(frequencies, totalCount))
  }

  private def computeCustomEdges(): Array[Double] = {
    throw new UnsupportedOperationException("Custom edges not yet implemented")
  }

  private def computeEqualWidthEdges(filteredData: DataFrame,
                                     numericCol: org.apache.spark.sql.Column): Array[Double] = {
    val stats = filteredData.select(numericCol).agg(
      min(column).alias("min_val"),
      max(column).alias("max_val")
    ).collect()(0)

    val minVal = stats.getAs[Number]("min_val")
    val maxVal = stats.getAs[Number]("max_val")

    if (minVal == null || maxVal == null) {
      return Array.empty[Double]
    }

    val minDouble = minVal.doubleValue()
    val maxDouble = maxVal.doubleValue()
    val binWidth = (maxDouble - minDouble) / binCount.get

    Array.tabulate(binCount.get + 1)(i => minDouble + i * binWidth)
  }

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): HistogramBinnedMetric = {
    state match {
      case Some(theState) =>
        val value: Try[DistributionBinned] = Try {
          val countColumnName = theState.frequencies.schema.fields
            .find(field => field.dataType == LongType && field.name != column)
            .map(_.name)
            .getOrElse(throw new IllegalStateException(s"Count column not found in the frequencies DataFrame"))

          val binCount = theState.frequencies.count()

          val histogramDetails = theState.frequencies.collect()
            .map { row =>
              val binValue = row.getAs[String](column)
              val absolute = row.getAs[Long](countColumnName)
              val ratio = absolute.toDouble / theState.numRows
              binValue -> DistributionValue(absolute, ratio)
            }.toMap

          // Convert to BinData objects
          val binDataSeq = (0 until storedEdges.length - 1).map { binIndex =>
            val binStart = storedEdges(binIndex)
            val binEnd = storedEdges(binIndex + 1)
            val distValue = histogramDetails.get(binIndex.toString).getOrElse(DistributionValue(0, 0.0))
            BinData(binStart, binEnd, distValue.absolute, distValue.ratio)
          }.toVector

          // Add NullValue bin if it exists
          val finalBins = if (histogramDetails.contains(Histogram.NullFieldReplacement)) {
            val nullDistValue = histogramDetails(Histogram.NullFieldReplacement)
            val nullBin = BinData(Double.NegativeInfinity, Double.PositiveInfinity,
                                 nullDistValue.absolute, nullDistValue.ratio)
            binDataSeq :+ nullBin
          } else {
            binDataSeq
          }

          DistributionBinned(finalBins, binCount)
        }

        HistogramBinnedMetric(column, value)

      case None =>
        HistogramBinnedMetric(column, Failure(Analyzers.emptyStateException(this)))
    }
  }

  override def toFailureMetric(exception: Exception): HistogramBinnedMetric = {
    HistogramBinnedMetric(column, Failure(MetricCalculationException.wrapIfNecessary(exception)))
  }

  override def preconditions: Seq[StructType => Unit] = {
    PARAM_CHECK :: Preconditions.hasColumn(column) :: Nil
  }
}

object HistogramBinned {
  val DefaultBinCount = 10
  val MaximumAllowedDetailBins = 1000

  // Note: Reuse Histogram.Count and Histogram.Sum for aggregation
  // since after binning, it is working with categorical bin indices
}
