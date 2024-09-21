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

import breeze.linalg.DenseVector


/**
 * Base class for detecting anomalies based on the values' rate of change.
 * The order of the difference can be set manually.
 * If it is set to 0, this strategy acts like the [[SimpleThresholdStrategy]].
 *                        Set to 1 it calculates the difference between two consecutive values.
 */
trait BaseChangeStrategy
  extends AnomalyDetectionStrategy with AnomalyDetectionStrategyWithExtendedResults {

  def maxRateDecrease: Option[Double]
  def maxRateIncrease: Option[Double]
  def order: Int

  require(maxRateDecrease.isDefined || maxRateIncrease.isDefined,
    "At least one of the two limits (maxRateDecrease or maxRateIncrease) has to be specified.")

  require(maxRateDecrease.getOrElse(Double.MinValue) <= maxRateIncrease.getOrElse(Double.MaxValue),
    "The maximal rate of increase has to be bigger than the maximal rate of decrease.")

  require(order >= 0, "Order of derivative cannot be negative.")


  /**
   * Calculates the absolute change with respect to the specified order.
   * If the order is set to 1, the resulting value for a point at index i
   * is equal to dataSeries (i) - dataSeries(i - 1).
   * Higher orders are calculated by calling diff recursively.
   * Note that this difference cannot be calculated for the first [[order]] elements in the vector.
   * The resulting vector is therefore smaller by [[order]] elements.
   *
   * @param dataSeries The values contained in a DenseVector[Double]
   * @param order      The order of the derivative.
   * @return A vector with the resulting rates of change for all values
   *         except the first [[order]] elements.
   */
  def diff(dataSeries: DenseVector[Double], order: Int): DenseVector[Double] = {
    require(order >= 0, "Order of diff cannot be negative")
    if (order == 0 || dataSeries.length == 0) {
      dataSeries
    } else {
      val valuesRight = dataSeries.slice(1, dataSeries.length)
      val valuesLeft = dataSeries.slice(0, dataSeries.length - 1)
      diff(valuesRight - valuesLeft, order - 1)
    }
  }

  /**
   * Search for anomalies in a series of data points. This function uses the
   * detectWithExtendedResults function and then filters and maps to return only anomaly data point objects.
   *
   * If there aren't enough data points preceding the searchInterval,
   * it may happen that the interval's first elements (depending on the specified order)
   * can't be flagged as anomalies.
   *
   * @param dataSeries     The data contained in a Vector of Doubles
   * @param searchInterval The indices between which anomalies should be detected. [a, b).
   * @return The indices of all anomalies in the interval and their corresponding wrapper object.
   */
  override def detect(
    dataSeries: Vector[Double],
    searchInterval: (Int, Int))
  : Seq[(Int, Anomaly)] = {

    detectWithExtendedResults(dataSeries, searchInterval)
      .filter { case (_, anomDataPoint) => anomDataPoint.isAnomaly }
      .map { case (i, anomDataPoint) =>
        (i, Anomaly(Some(anomDataPoint.dataMetricValue), anomDataPoint.confidence, anomDataPoint.detail))
      }
  }

  /**
   * Search for anomalies in a series of data points, returns extended results.
   *
   * If there aren't enough data points preceding the searchInterval,
   * it may happen that the interval's first elements (depending on the specified order)
   * can't be flagged as anomalies.
   *
   * @param dataSeries     The data contained in a Vector of Doubles
   * @param searchInterval The indices between which anomalies should be detected. [a, b).
   * @return The indices of all anomalies in the interval and their corresponding wrapper object
   *         with extended results.
   */
  override def detectWithExtendedResults(
    dataSeries: Vector[Double],
    searchInterval: (Int, Int))
  : Seq[(Int, AnomalyDetectionDataPoint)] = {
    val (start, end) = searchInterval

    require(start <= end,
      "The start of the interval cannot be larger than the end.")

    val startPoint = Seq(start - order, 0).max
    val data = diff(DenseVector(dataSeries.slice(startPoint, end): _*), order).data

    val lowerBound = maxRateDecrease.getOrElse(Double.MinValue)
    val upperBound = maxRateIncrease.getOrElse(Double.MaxValue)


    data.zipWithIndex.map {
      case (change, index) =>
        val outputSequenceIndex = index + startPoint + order
        val value = dataSeries(outputSequenceIndex)
        val (detail, isAnomaly) = if (change < lowerBound || change > upperBound) {
          (Some(s"[AbsoluteChangeStrategy]: Change of $change is not in bounds [" +
            s"$lowerBound, " +
            s"$upperBound]. Order=$order"), true)
        }
        else {
          (None, false)
        }
        (outputSequenceIndex, AnomalyDetectionDataPoint(value, change,
          Threshold(lowerBound = Bound(lowerBound), upperBound = Bound(upperBound)), isAnomaly, 1.0, detail))
    }
  }
}
