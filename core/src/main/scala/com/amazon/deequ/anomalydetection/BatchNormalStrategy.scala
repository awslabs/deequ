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

import breeze.stats.meanAndVariance


/**
  * Detects anomalies based on the mean and standard deviation of all available values.
  * Assumes that the data is normally distributed.
  *
  * @param lowerDeviationFactor Detect anomalies if they are
  *                             smaller than mean - lowerDeviationFactor * stdDev
  * @param upperDeviationFactor Detect anomalies if they are
  *                             bigger than mean + upperDeviationFactor * stdDev
  * @param includeInterval      Whether or not values inside the detection interval should be
  *                             included in the calculation of the mean/ stdDev
  */
case class BatchNormalStrategy(
  lowerDeviationFactor: Option[Double] = Some(3.0),
  upperDeviationFactor: Option[Double] = Some(3.0),
  includeInterval: Boolean = false) extends AnomalyDetectionStrategy {

  require(lowerDeviationFactor.isDefined || upperDeviationFactor.isDefined,
    "At least one factor has to be specified.")

  require(lowerDeviationFactor.getOrElse(1.0) >= 0 && upperDeviationFactor.getOrElse(1.0) >= 0,
    "Factors cannot be smaller than zero.")


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

    require(searchStart <= searchEnd, "The start of the interval can't be larger than the end.")

    require(dataSeries.nonEmpty, "Data series is empty. Can't calculate mean/ stdDev.")

    val searchIntervalLength = searchEnd - searchStart

    require(includeInterval || searchIntervalLength < dataSeries.length,
      "Excluding values in searchInterval from calculation but not enough values remain to " +
      "calculate mean and stdDev.")

    val mAV = if (includeInterval) {
      meanAndVariance(dataSeries)
    } else {
      val valuesBeforeInterval = dataSeries.slice(0, searchStart)
      val valuesAfterInterval = dataSeries.slice(searchEnd, dataSeries.length)
      val dataSeriesWithoutInterval = valuesBeforeInterval ++ valuesAfterInterval

      meanAndVariance(dataSeriesWithoutInterval)
    }

    val mean = mAV.mean
    val stdDev = mAV.stdDev

    val upperBound = mean + upperDeviationFactor.getOrElse(Double.MaxValue) * stdDev
    val lowerBound = mean - lowerDeviationFactor.getOrElse(Double.MaxValue) * stdDev

    dataSeries.zipWithIndex
      .slice(searchStart, searchEnd)
      .filter { case (value, _) => value > upperBound || value < lowerBound }
      .map { case (value, index) =>

        val detail = Some(s"[BatchNormalStrategy]: Value $value is not in " +
          s"bounds [$lowerBound, $upperBound].")

        (index, Anomaly(Option(value), 1.0, detail))
      }
  }
}
