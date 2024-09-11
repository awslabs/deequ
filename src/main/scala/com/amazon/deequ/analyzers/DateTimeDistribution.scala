/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.analyzers

import java.time.Instant
import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isDateType}
import com.amazon.deequ.analyzers.runners.MetricCalculationException
import com.amazon.deequ.metrics.{Distribution, DistributionValue, HistogramMetric}
import org.apache.spark.sql.DeequFunctions.dateTimeDistribution
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Row}

import scala.util.{Failure, Success}

object DistributionInterval extends Enumeration {
  val QUARTER_HOUR, HOURLY, DAILY, WEEKLY, MONTHLY = Value
}

case class DateTimeDistributionState(distribution: Map[(Instant, Instant), Long])
  extends State[DateTimeDistributionState] {

  override def sum(other: DateTimeDistributionState): DateTimeDistributionState = {

    DateTimeDistributionState(distribution ++ other.distribution.map {
      case (k, v) => k -> (v + distribution.getOrElse(k, 0L))
    })
  }
}

object DateTimeDistributionState {

  def computeStateFromResult(
    result: Map[Long, Long],
    frequency: Long
  ): Map[(Instant, Instant), Long] = {
    result.map({
      case (x, y) => (Instant.ofEpochMilli(x), Instant.ofEpochMilli(x + frequency - 1L)) -> y
    })
  }

  def toDistribution(histogram: DateTimeDistributionState): Distribution = {
    val totalCount = histogram.distribution.foldLeft(0L)(_ + _._2)
    Distribution(
      histogram.distribution.map {
        case (x, y) =>
          ("(" + x._1.toString + " to " + x._2.toString + ")") -> DistributionValue(y, y.toDouble / totalCount)
      },
      totalCount
    )
  }
}

/**
 *
 * @param column   : column on which distribution analysis is to be performed
 * @param interval : interval of the distribution;
 * @param where    : optional filter condition
 */
case class DateTimeDistribution(
                                 column: String,
                                 interval: Long,
                                 where: Option[String] = None)
  extends ScanShareableAnalyzer[DateTimeDistributionState, HistogramMetric]
    with FilterableAnalyzer {

  /** Defines the aggregations to compute on the data */
  override private[deequ] def aggregationFunctions(): Seq[Column] = {
    dateTimeDistribution(conditionalSelection(column, where), interval) :: Nil
  }

  /** Computes the state from the result of the aggregation functions */
  override private[deequ] def fromAggregationResult(
    result: Row,
    offset: Int
  ): Option[DateTimeDistributionState] = {
    ifNoNullsIn(result, offset) { _ =>
      DateTimeDistributionState(
        DateTimeDistributionState.computeStateFromResult(
          Map.empty[Long, Long] ++ result.getMap(0),
          interval
        )
      )
    }
  }

  override def preconditions: Seq[StructType => Unit] = {
    hasColumn(column) +: isDateType(column) +: super.preconditions
  }

  override def filterCondition: Option[String] = where

  /**
   * Compute the metric from the state (sufficient statistics)
   *
   * @param state wrapper holding a state of type S (required due to typing issues...)
   * @return
   */
  override def computeMetricFrom(state: Option[DateTimeDistributionState]): HistogramMetric = {
    state match {
      case Some(histogram) =>
        HistogramMetric(column, Success(DateTimeDistributionState.toDistribution(histogram)))
      case _ =>
        toFailureMetric(emptyStateException(this))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception): HistogramMetric = {
    HistogramMetric(column, Failure(MetricCalculationException.wrapIfNecessary(failure)))
  }
}

object DateTimeDistribution {
  def apply(column: String,
            interval: DistributionInterval.Value,
            where: Option[String]): DateTimeDistribution =
    new DateTimeDistribution(column, interval = getDateTimeAggIntervalValue(interval), where)

  def apply(column: String,
            interval: DistributionInterval.Value): DateTimeDistribution =
    new DateTimeDistribution(column, interval = getDateTimeAggIntervalValue(interval), None)

  def getDateTimeAggIntervalValue(interval: DistributionInterval.Value): Long = {
    interval match {
      case DistributionInterval.QUARTER_HOUR => 900000L // 15 Minutes
      case DistributionInterval.HOURLY => 3600000L // 60 Minutes
      case DistributionInterval.DAILY => 86400000L // 24 Hours
      case DistributionInterval.WEEKLY => 604800000L
      case _ => 604800000L // 7 * 24 Hours
    }
  }

}
