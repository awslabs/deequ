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

import scala.collection.mutable.ArrayBuffer
import scala.math.sqrt


/**
  * Detects anomalies based on the running mean and standard deviation.
  * Anomalies can be excluded from the computation to not affect
  * the calculated mean/ standard deviation.
  * Assumes that the data is normally distributed.
  *
  * @param lowerDeviationFactor  Detect anomalies if they are smaller than
  *                              mean - lowerDeviationFactor * stdDev
  * @param upperDeviationFactor  Detect anomalies if they are bigger than
  *                              mean + upperDeviationFactor * stdDev
  * @param ignoreStartPercentage Percentage of data points after start in which no anomalies should
  *                              be detected (mean and stdDev are probably
  *                              not representative before).
  * @param ignoreAnomalies       If set to true, ignores anomalous points in mean
  *                              and variance calculation.
  */
case class OnlineNormalStrategy(
  lowerDeviationFactor: Option[Double] = Some(3.0),
  upperDeviationFactor: Option[Double] = Some(3.0),
  ignoreStartPercentage: Double = 0.1,
  ignoreAnomalies: Boolean = true) extends AnomalyDetectionStrategy {

  require(lowerDeviationFactor.isDefined || upperDeviationFactor.isDefined,
    "At least one factor has to be specified.")

  require(lowerDeviationFactor.getOrElse(1.0) >= 0 && upperDeviationFactor.getOrElse(1.0) >= 0,
    "Factors cannot be smaller than zero.")

  require(ignoreStartPercentage >= 0 && ignoreStartPercentage <= 1.0,
    "Percentage of start values to ignore must be in interval [0, 1].")

  /**
    * Wrapper class containing mean and stdDev of values up to a specific point and if this point
    * is an anomaly. Note that anomalies may not influence these values.
    */
  case class OnlineCalculationResult(mean: Double, stdDev: Double, isAnomaly: Boolean)

  /**
    * Calculation of incremental values from
    * http://people.ds.cam.ac.uk/fanf2/hermes/doc/antiforgery/stats.pdf.
    * For each value, a new mean and standard deviation get computed based on the previous
    * calculation. To calculate the standard deviation, a helper variable Sn is used.
    *
    * @param dataSeries     The data contained in a Vector of Doubles
    * @param searchInterval The indices between which anomalies should be detected. [a, b).
    * @return A triple containing current mean, stdDev and if the point is an anomaly.
    */
  def computeStatsAndAnomalies(
    dataSeries: Vector[Double],
    searchInterval: (Int, Int) = (0, Int.MaxValue)): Seq[OnlineCalculationResult] = {
    val ret: ArrayBuffer[OnlineCalculationResult] = ArrayBuffer()

    var currentMean = 0.0
    var currentVariance = 0.0

    var Sn = 0.0

    val numValuesToSkip = dataSeries.length * ignoreStartPercentage

    for (currentIndex <- dataSeries.indices) {
      val currentValue = dataSeries(currentIndex)

      val lastMean = currentMean
      val lastVariance = currentVariance
      val lastSn = Sn


      if (currentIndex == 0) {
        currentMean = currentValue
      } else {
        currentMean = lastMean + (1.0 / (currentIndex + 1)) * (currentValue - lastMean)
      }

      Sn += (currentValue - lastMean) * (currentValue - currentMean)
      currentVariance = Sn / (currentIndex + 1)
      val stdDev = sqrt(currentVariance)

      val upperBound = currentMean + upperDeviationFactor.getOrElse(Double.MaxValue) * stdDev
      val lowerBound = currentMean - lowerDeviationFactor.getOrElse(Double.MaxValue) * stdDev

      val (searchStart, searchEnd) = searchInterval

      if (currentIndex < numValuesToSkip ||
        currentIndex < searchStart || currentIndex >= searchEnd ||
        (currentValue <= upperBound && currentValue >= lowerBound)) {
        ret += OnlineCalculationResult(currentMean, stdDev, isAnomaly = false)
      } else {
        if (ignoreAnomalies) {
          // Anomaly doesn't affect mean and variance
          currentMean = lastMean
          currentVariance = lastVariance
          Sn = lastSn
        }
        ret += OnlineCalculationResult(currentMean, stdDev, isAnomaly = true)
      }
    }
    ret
  }


  /**
    * Search for anomalies in a series of data points.
    *
    * @param dataSeries     The data contained in a Vector of Doubles
    * @param searchInterval The indices between which anomalies should be detected. [a, b).
    * @return The indices of all anomalies in the interval and their corresponding wrapper object.
    */
  override def detect(
      dataSeries: Vector[Double],
      searchInterval: (Int, Int))
    : Seq[(Int, Anomaly)] = {

    val (searchStart, searchEnd) = searchInterval

    require(searchStart <= searchEnd, "The start of the interval can't be larger than the end.")

    computeStatsAndAnomalies(dataSeries, searchInterval)
      .zipWithIndex
      .slice(searchStart, searchEnd)
      .filter { case (result, _) => result.isAnomaly }
      .map { case (calcRes, index) =>
        val lowerBound =
          calcRes.mean - lowerDeviationFactor.getOrElse(Double.MaxValue) * calcRes.stdDev
        val upperBound =
          calcRes.mean + upperDeviationFactor.getOrElse(Double.MaxValue) * calcRes.stdDev

        val detail = Some(s"[OnlineNormalStrategy]: Value ${dataSeries(index)} is not in " +
          s"bounds [$lowerBound, $upperBound].")

        (index, Anomaly(Option(dataSeries(index)), 1.0, detail))
      }
  }
}
