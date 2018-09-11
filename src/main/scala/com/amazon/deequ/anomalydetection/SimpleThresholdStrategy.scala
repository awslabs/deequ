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

/**
  * A simple anomaly detection method that checks if values are in a specified range.
  *
  * @param lowerBound Lower bound of accepted range of values
  * @param upperBound Upper bound of accepted range of values
  */
case class SimpleThresholdStrategy(
    lowerBound: Double = Double.MinValue,
    upperBound: Double)
  extends AnomalyDetectionStrategy {

  require(lowerBound <= upperBound, "The lower bound must be smaller or equal to the upper bound.")

  /**
    * Search for anomalies in a series of data points.
    *
    * @param dataSeries     The data contained in a Vector of Doubles
    * @param searchInterval The indices between which anomalies should be detected. [a, b).
    * @return The indices of all anomalies in the interval and their corresponding wrapper object.
    */
  override def detect(
    dataSeries: Vector[Double],
    searchInterval: (Int, Int)): Seq[(Int, Anomaly)] = {

    val (searchStart, searchEnd) = searchInterval

    require (searchStart <= searchEnd, "The start of the interval can't be larger than the end.")

    dataSeries.zipWithIndex
      .slice(searchStart, searchEnd)
      .filter { case (value, _) => value < lowerBound || value > upperBound }
      .map { case (value, index) =>

        val detail = Some(s"[SimpleThresholdStrategy]: Value $value is not in " +
          s"bounds [$lowerBound, $upperBound]")

        (index, Anomaly(Option(value), 1.0, detail))
      }
  }
}
