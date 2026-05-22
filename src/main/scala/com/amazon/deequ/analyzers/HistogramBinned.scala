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
import org.apache.spark.sql.Column
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

/** State for HistogramBinned that carries bin edges alongside frequencies. */
case class BinnedFrequencies(
  frequencies: DataFrame,
  numRows: Long,
  edges: Array[Double]
) extends State[BinnedFrequencies] {

  override def sum(other: BinnedFrequencies): BinnedFrequencies = {
    throw new UnsupportedOperationException(
      "BinnedFrequencies states cannot be merged. Use calculate() on the full dataset.")
  }
}

/**
 * Histogram analyzer for numerical data with binning support.
 * Currently supports equal-width bins.
 */
case class HistogramBinned(
                            override val column: String,
                            binCount: Option[Int] = None,
                            customEdges: Option[Array[Double]] = None,
                            includeOverflowBins: Boolean = false,
                            override val where: Option[String] = None,
                            override val computeFrequenciesAsRatio: Boolean = true,
                            // Reuse Histogram's AggregateFunction implementations since just grouping
                            // by bin index (which becomes categorical data)
                            override val aggregateFunction: AggregateFunction = Histogram.Count)
  extends HistogramBase(column, where, computeFrequenciesAsRatio, aggregateFunction)
    with Analyzer[BinnedFrequencies, HistogramBinnedMetric] {

  require(binCount.isDefined ^ customEdges.isDefined,
    "Must specify either binCount (equal-width) or customEdges (custom)")

  require(customEdges.forall(_.length >= 2),
    "Custom edges must have at least 2 values")

  // Array has reference equality in Scala; override so AnalyzerContext map
  // lookups and serde round-trips compare contents, not references.
  override def equals(obj: Any): Boolean = obj match {
    case other: HistogramBinned =>
      column == other.column &&
        binCount == other.binCount &&
        customEdges.map(_.toSeq) == other.customEdges.map(_.toSeq) &&
        includeOverflowBins == other.includeOverflowBins &&
        where == other.where &&
        computeFrequenciesAsRatio == other.computeFrequenciesAsRatio &&
        aggregateFunction == other.aggregateFunction
    case _ => false
  }

  override def hashCode(): Int = {
    (column, binCount, customEdges.map(_.toSeq), includeOverflowBins,
      where, computeFrequenciesAsRatio, aggregateFunction).hashCode()
  }

  private var storedEdges: Array[Double] = _

  protected[this] val PARAM_CHECK: StructType => Unit = { _ =>
    binCount.foreach { count =>
      if (includeOverflowBins && count < 3) {
        throw new IllegalAnalyzerParameterException(
          "binCount must be at least 3 when includeOverflowBins is true (2 overflow + 1 interior)")
      }
      if (count > HistogramBinned.MaximumAllowedDetailBins) {
        throw new IllegalAnalyzerParameterException(s"Cannot return histogram values for more " +
          s"than ${HistogramBinned.MaximumAllowedDetailBins} bins")
      }
    }
    customEdges.foreach { edges =>
      // Count overflow bins that will actually be added (only if not already present)
      val overflowExtra = if (includeOverflowBins) {
        (if (edges.sorted.head != Double.NegativeInfinity) 1 else 0) +
        (if (edges.sorted.last != Double.PositiveInfinity) 1 else 0)
      } else 0
      val numBins = edges.length - 1 + overflowExtra
      if (numBins > HistogramBinned.MaximumAllowedDetailBins) {
        throw new IllegalAnalyzerParameterException(s"Cannot return histogram values for more " +
          s"than ${HistogramBinned.MaximumAllowedDetailBins} bins")
      }
    }
  }

  override def computeStateFrom(data: DataFrame,
                                filterCondition: Option[String] = None): Option[BinnedFrequencies] = {

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
    } else if (customEdges.isDefined || includeOverflowBins) {
      val edges = storedEdges

      // Builds a balanced binary decision tree of when/otherwise expressions,
      // giving O(log n) comparisons per row instead of O(n) for a linear chain.
      // This matches Spark's own Bucketizer.binarySearchForBuckets approach
      // (see org.apache.spark.ml.feature.Bucketizer) and matters at scale:
      // e.g. 100 bins x 1B rows = ~7 comparisons/row vs ~99 for linear scan.
      def buildBinaryCondition(binIndices: Seq[Int]): Column = {
        if (binIndices.isEmpty) {
          lit(HistogramBinned.OutOfRangeReplacement)
        } else if (binIndices.size == 1) {
          // Single bin, check if value falls in this bin
          val i = binIndices.head
          val isLastBin = i == edges.length - 2
          // Last interior bin is inclusive [a, b] when overflow is enabled,
          // so data max stays in interior. Identified by: next bin ends at +Inf.
          val isLastInterior = includeOverflowBins && i < edges.length - 2 &&
            edges(i + 2) == Double.PositiveInfinity
          val condition = if (isLastBin || isLastInterior) {
            // Last bin and last interior bin are inclusive [a, b]
            numericCol >= edges(i) && numericCol <= edges(i + 1)
          } else {
            // All other bins exclude upper bound [a, b)
            numericCol >= edges(i) && numericCol < edges(i + 1)
          }
          when(condition, lit(i.toString)).otherwise(lit(HistogramBinned.OutOfRangeReplacement))
        } else {
          // Split bins in half and create decision tree
          val mid = binIndices.size / 2
          val (leftBins, rightBins) = binIndices.splitAt(mid)
          val splitEdge = edges(rightBins.head)

          // When the right branch starts with the overflow bin, use <= so that
          // values on the boundary stay in the last interior bin (left side)
          val isOverflowSplit = includeOverflowBins &&
            edges(rightBins.head + 1) == Double.PositiveInfinity
          if (isOverflowSplit) {
            when(numericCol <= splitEdge, buildBinaryCondition(leftBins))
              .otherwise(buildBinaryCondition(rightBins))
          } else {
            when(numericCol < splitEdge, buildBinaryCondition(leftBins))
              .otherwise(buildBinaryCondition(rightBins))
          }
        }
      }

      val allBinIndices = (0 until edges.length - 1)
      val binCondition = buildBinaryCondition(allBinIndices)
      val finalCondition = when(numericCol.isNull, lit(Histogram.NullFieldReplacement))
        .otherwise(binCondition)

      filteredData.withColumn(column, finalCondition)
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

    Some(BinnedFrequencies(frequencies, totalCount, storedEdges))
  }

  private def computeCustomEdges(): Array[Double] = {
    val sorted = customEdges.get.sorted
    addOverflowEdges(sorted)
  }

  private def addOverflowEdges(edges: Array[Double]): Array[Double] = {
    if (!includeOverflowBins) return edges
    val withLeft = if (edges.head != Double.NegativeInfinity) Double.NegativeInfinity +: edges else edges
    val withBoth = if (withLeft.last != Double.PositiveInfinity) withLeft :+ Double.PositiveInfinity else withLeft
    withBoth.toArray
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
    val interiorBins = if (includeOverflowBins) binCount.get - 2 else binCount.get
    val binWidth = (maxDouble - minDouble) / interiorBins

    val edges = Array.tabulate(interiorBins + 1)(i => minDouble + i * binWidth)
    addOverflowEdges(edges)
  }

  override def computeMetricFrom(state: Option[BinnedFrequencies]): HistogramBinnedMetric = {
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

          val edges = theState.edges

          // Convert to BinData objects
          val binDataSeq = (0 until edges.length - 1).map { binIndex =>
            val binStart = edges(binIndex)
            val binEnd = edges(binIndex + 1)
            val distValue = histogramDetails.get(binIndex.toString).getOrElse(DistributionValue(0, 0.0))
            BinData(binStart, binEnd, distValue.absolute, distValue.ratio)
          }.toVector

          val nullCount = histogramDetails.get(Histogram.NullFieldReplacement)
            .map(_.absolute).getOrElse(0L)

          DistributionBinned(binDataSeq, binDataSeq.size, nullCount)
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
  // Out-of-range values get this label; ignored in computeMetricFrom (effectively dropped)
  val OutOfRangeReplacement = "OutOfRange"

  // Note: Reuse Histogram.Count and Histogram.Sum for aggregation
  // since after binning, it is working with categorical bin indices
}
