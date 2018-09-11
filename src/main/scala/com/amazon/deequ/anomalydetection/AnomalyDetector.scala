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

package com.amazon.deequ.anomalydetection

import scala.collection.Searching._

case class DataPoint[M](time: Long, metricValue: Option[M])

/**
  * Detects anomalies in a time series given a strategy how to do so.
  * This class is mainly responsible for the preprocessing of the given time series.
  *
  * @param strategy An implementation of AnomalyDetectionStrategy that needs preprocessed values.
  */
case class AnomalyDetector(strategy: AnomalyDetectionStrategy) {

  /**
    * Given a sequence of metrics and a current value, detects if there is an anomaly by using the
    * given algorithm.
    *
    * @param historicalDataPoints     Sequence of tuples (Points in time with corresponding Metric).
    * @param newPoint       A new data point to check if there are anomalies
    * @return
    */
  def isNewPointAnomalous(
      historicalDataPoints: Seq[DataPoint[Double]],
      newPoint: DataPoint[Double])
    : DetectionResult = {

    require(historicalDataPoints.nonEmpty, "historicalDataPoints must not be empty!")

    val sortedDataPoints = historicalDataPoints.sortBy(_.time)

    val firstDataPointTime = sortedDataPoints.head.time
    val lastDataPointTime = sortedDataPoints.last.time

    val newPointTime = newPoint.time

    require(lastDataPointTime < newPointTime,
      s"Can't decide which range to use for anomaly detection. New data point with time " +
          s"$newPointTime is in history range ($firstDataPointTime - $lastDataPointTime)!")

    val allDataPoints = sortedDataPoints :+ newPoint

    // Run anomaly
    val anomalies = detectAnomaliesInHistory(allDataPoints, (newPoint.time, Long.MaxValue))
      .anomalies

    // Create a Detection result with all anomalies
    DetectionResult(anomalies)
  }

  /**
    * Given a strategy, detects anomalies in a time series after some preprocessing.
    *
    * @param dataSeries     Sequence of tuples (Points in time with corresponding value).
    * @param searchInterval The interval in which anomalies should be detected. [a, b).
    * @return A wrapper object, containing all anomalies.
    */
  def detectAnomaliesInHistory(
      dataSeries: Seq[DataPoint[Double]],
      searchInterval: (Long, Long) = (Long.MinValue, Long.MaxValue))
    : DetectionResult = {

    def findIndexForBound(sortedTimestamps: Seq[Long], boundValue: Long): Int = {
      sortedTimestamps.search(boundValue).insertionPoint
    }

    val (searchStart, searchEnd) = searchInterval

    require (searchStart <= searchEnd,
      "The first interval element has to be smaller or equal to the last.")

    // Remove missing values and sort series by time
    val removedMissingValues = dataSeries.filter { _.metricValue.isDefined }
    val sortedSeries = removedMissingValues.sortBy { _.time }
    val sortedTimestamps = sortedSeries.map { _.time }

    // Find indices of lower and upper bound
    val lowerBoundIndex = findIndexForBound(sortedTimestamps, searchStart)
    val upperBoundIndex = findIndexForBound(sortedTimestamps, searchEnd)

    val anomalies = strategy.detect(
      sortedSeries.flatMap { _.metricValue }.toVector, (lowerBoundIndex, upperBoundIndex))

    DetectionResult(anomalies.map { case (index, anomaly) => (sortedTimestamps(index), anomaly) })
  }
}
