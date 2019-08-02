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
  * Detects anomalies based on the values' rate of change.
  * The order of the difference can be set manually.
  * If it is set to 0, this strategy acts like the [[SimpleThresholdStrategy]].
  *
  * RelativeRateOfChangeStrategy(Some(-10.0), Some(10.0), 1) for example
  * calculates the first discrete difference
  * and if some point's value changes by more than 10.0 Percent in one timestep, it flags it as an anomaly.
  *
  * @param maxRateDecrease Upper bound of accepted decrease (lower bound of increase).
  * @param maxRateIncrease Upper bound of accepted growth.
  * @param order           Order of the calculated difference.
  *                        Set to 1 it calculates the difference between two consecutive values.
  */
case class RelativeRateOfChangeStrategy(
    maxRateDecrease: Option[Double] = None,
    maxRateIncrease: Option[Double] = None,
    order: Int = 1)
  extends AnomalyDetectionStrategy {

  require(maxRateDecrease.isDefined || maxRateIncrease.isDefined,
    "At least one of the two limits (maxRateDecrease or maxRateIncrease) has to be specified.")

  require(maxRateDecrease.getOrElse(Double.MinValue) <= maxRateIncrease.getOrElse(Double.MaxValue),
    "The maximal rate of increase has to be bigger than the maximal rate of decrease.")

  require(order > 0, "Order of derivative cannot be zero or negative.")

  /**
    * Calculates the rate of change with respect to the specified order.
    * If the order is set to 1, the resulting value for a point at index i
    * is equal to dataSeries (i) - dataSeries(i - 1).
    * Higher orders are calculated by calling diff recursively.
    * Note that this difference cannot be calculated for the first element in the vector.
    * The resulting vector is therefore smaller by one element.
    *
    * @param dataSeries The values contained in a DenseVector[Double]
    * @param order      The order of the derivative.
    * @return A vector with the resulting rates of change for all values except the first.
    */
  def diff(dataSeries: DenseVector[Double], order: Int): DenseVector[Double] = {
    require(order > 0, "Order of diff cannot be zero or negative")
    if (dataSeries.length == 0) {
      dataSeries
    } else {
      val valuesRight = dataSeries.slice(order, dataSeries.length)
      val valuesLeft = dataSeries.slice(0, dataSeries.length - order)
      valuesRight / valuesLeft
    }
  }

  /**
    * Search for anomalies in a series of data points.
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

    require(searchInterval._1 <= searchInterval._2,
      "The start of the interval cannot be larger than the end.")

    val startPoint = Seq(searchInterval._1 - order, 0).max
    val data = diff(DenseVector(dataSeries.slice(startPoint, searchInterval._2): _*), order).data

    data.zipWithIndex.filter { case (value, _) =>
      (value - 1 < maxRateDecrease.getOrElse(Double.MinValue)
        || value - 1 > maxRateIncrease.getOrElse(Double.MaxValue))
    }.map { case (change, index) =>
      (index + startPoint + order, Anomaly(Option(dataSeries(index + startPoint + order)), 1.0,
        Some(s"[RelativeRateOfChangeStrategy]: Change of $change is not in bounds [" +
          s"${maxRateDecrease.getOrElse(Double.MinValue)}, " +
          s"${maxRateIncrease.getOrElse(Double.MaxValue)}]. Order=$order")))
    }
  }
}
