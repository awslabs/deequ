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

package com.amazon.deequ.anomalydetection.seasonal

import breeze.linalg.DenseVector
import breeze.optimize.{ApproximateGradientFunction, DiffFunction, LBFGSB}
import com.amazon.deequ.anomalydetection.{Anomaly, AnomalyDetectionDataPoint, AnomalyDetectionStrategy, AnomalyDetectionStrategyWithExtendedResults, Threshold, Bound}

import collection.mutable.ListBuffer

object HoltWinters {

  object SeriesSeasonality extends Enumeration {
    val Daily, Weekly, Yearly: Value = Value
  }

  object MetricInterval extends Enumeration {
    val Hourly, Daily, Monthly: Value = Value
  }

  private[seasonal] case class ModelResults(
    forecasts: Seq[Double],
    level: Seq[Double],
    trend: Seq[Double],
    seasonality: Seq[Double],
    residuals: Seq[Double]
  )

  private[seasonal] case class HoltWintersParameters(
    alpha: Double,
    beta: Double,
    gamma: Double
  )

}

class HoltWinters(seriesPeriodicity: Int)
  extends AnomalyDetectionStrategy with AnomalyDetectionStrategyWithExtendedResults {

  import HoltWinters._

  /**
   * Detects anomalies based on additive Holt-Winters model. The methods has two
   * parameters, one for the metric frequency, as in how often the metric of interest
   * is computed (e.g. daily) and one for the expected metric seasonality which
   * defines the longest cycle in series. This quantity is also referred to as periodicity.
   *
   * For example, if a metric is produced daily and repeats itself every Monday, then the
   * model should be created with a Daily metric interval and a Weekly seasonality parameter.
   *
   * @param metricsInterval : How often a metric is available
   * @param seasonality     : Cycle length (or periodicity) of the metric
   */
  def this(metricsInterval: HoltWinters.MetricInterval.Value,
           seasonality: HoltWinters.SeriesSeasonality.Value) =
    this(seasonality -> metricsInterval match {
      case (HoltWinters.SeriesSeasonality.Daily, HoltWinters.MetricInterval.Hourly) => 24
      case (HoltWinters.SeriesSeasonality.Weekly, HoltWinters.MetricInterval.Daily) => 7
      case (HoltWinters.SeriesSeasonality.Yearly, HoltWinters.MetricInterval.Monthly) => 12
    })

  /**
    * Triple exponential smoothing with additive trend and seasonality
    * ETS(A, A) according to https://otexts.org/fpp2/taxonomy.html
    *
    * References:
    * - https://otexts.org/fpp2/holt-winters.html
    * - https://gist.github.com/andrequeiroz/5888967
    *
    * @param series: input time series
    * @param periodicity: periodicity of series
    * @param numberOfPointsToForecast: number of data points to forecast
    * @return (forecasts, one step ahead residual SD)
    */
  private[seasonal] def additiveHoltWinters(
      series: Seq[Double],
      periodicity: Int,
      numberOfPointsToForecast: Int,
      alpha: Double,
      beta: Double,
      gamma: Double)
    : ModelResults = {

    val firstPeriodSum = series.take(periodicity).sum
    val secondPeriodSum = series.slice(periodicity, 2 * periodicity).sum

    // Mean of first period
    val level = ListBuffer(firstPeriodSum / periodicity.toDouble)

    // Mean of second period - mean of first period
    val trend = ListBuffer(
      (secondPeriodSum - firstPeriodSum) / (periodicity.toDouble * periodicity.toDouble)
    )

    // First `periodicity` data points - estimated level
    val seriesMinusEstimatedLevel = series.take(periodicity).map(_ - level.head)
    val seasonality = ListBuffer(seriesMinusEstimatedLevel: _*)
    // Running signal estimate
    val y = ListBuffer(level.head + trend.head + seasonality.head)
    // Input `series`, `numberOfPointsToForecast` forecasts will be appended here
    val Y = ListBuffer(series: _*)

    for (t <- 0 until series.size + numberOfPointsToForecast) {
      if (t >= series.size) {
        Y.append(level.last + trend.last + seasonality(seasonality.size - periodicity))
      }

      level.append(alpha * (Y(t) - seasonality(t)) + (1 - alpha) * (level(t) + trend(t)))
      trend.append(beta * (level(t + 1) - level(t)) + (1 - beta) * trend(t))
      seasonality.append(gamma * (Y(t) - level(t) - trend(t)) + (1 - gamma) * seasonality(t))

      y.append(level(t + 1) + trend(t + 1) + seasonality(t + 1))
    }

    // Forecast residuals
    val residuals = y.zip(series)
      .map { case (modelForecast, seriesValue) =>
        seriesValue - modelForecast
      }
    val forecasted = Y.drop(series.size)

    ModelResults(forecasted, level, trend, seasonality, residuals)
  }

  private def modelSelectionFor(
      dataSeries: Seq[Double],
      numberOfObservationsToForecast: Int)
    : HoltWintersParameters = {

    // Solver with parameter bounds
    val solver = new LBFGSB(
      lowerBounds = DenseVector[Double](0, 0, 0),
      upperBounds = DenseVector[Double](1, 1, 1)
    )

    // Initial smoothing parameters for level, trend, and seasonality respectively
    val initialParameters = DenseVector[Double](0.3, 0.1, 0.1)

    val objective = new DiffFunction[DenseVector[Double]] {
      override def calculate(x: DenseVector[Double]): (Double, DenseVector[Double]) = {
        val modelResults = additiveHoltWinters(
          dataSeries, seriesPeriodicity, numberOfObservationsToForecast,
          alpha = x(0), beta = x(1), gamma = x(2)
        )
        val rss = modelResults.residuals.map(math.pow(_, 2)).sum
        val gradient = DenseVector.zeros[Double](x.length)

        rss -> gradient
      }
    }

    val objectiveWithApproximateGradients =
      new ApproximateGradientFunction[Int, DenseVector[Double]](objective)

    val optimalParameters = solver.minimize(objectiveWithApproximateGradients, initialParameters)
    HoltWintersParameters(
      alpha = optimalParameters(0),
      beta = optimalParameters(1),
      gamma = optimalParameters(2)
    )
  }


  /**
   * This function is renamed to add 'withExtendedResults' to the name.
   * The functionality no longer filters out non anomalies, but instead leaves a flag
   * of whether it's anomaly or not. The previous anomaly detection strategy uses this refactored function
   * and then does the filtering to remove non anomalies and maps to previous anomaly objects.
   * The new anomaly detection strategy with extended results uses this function and does not filter on it.
   */
  private def findAnomaliesWithExtendedResults(
                             testSeries: Vector[Double],
                             forecasts: Seq[Double],
                             startIndex: Int,
                             residualSD: Double)
  : Seq[(Int, AnomalyDetectionDataPoint)] = {

    testSeries.zip(forecasts).zipWithIndex
      .collect { case ((inputValue, forecastedValue), detectionIndex) =>
        val anomalyMetricValue = math.abs(inputValue - forecastedValue)
        val upperBound = 1.96 * residualSD

        val (detail, isAnomaly) = if (anomalyMetricValue > upperBound) {
          (Some(s"Forecasted $forecastedValue for observed value $inputValue"), true)
        } else {
          (None, false)
        }
        detectionIndex + startIndex -> AnomalyDetectionDataPoint(
          dataMetricValue = inputValue,
          anomalyMetricValue = anomalyMetricValue,
          anomalyThreshold = Threshold(upperBound = Bound(upperBound)),
          isAnomaly = isAnomaly,
          confidence = 1.0,
          detail = detail
        )
    }
  }

  /**
    * Search for anomalies in a series of data points. This function uses the
    * detectWithExtendedResults function and then filters and maps to return only anomaly objects.
    *
    * @param dataSeries     The data contained in a Vector of Doubles.
    * @param searchInterval The indices between which anomalies should be detected. [a, b).
    * @return The indices of all anomalies in the interval and their corresponding wrapper object.
    *
    */
  override def detect(
      dataSeries: Vector[Double],
      searchInterval: (Int, Int) = (0, Int.MaxValue))
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
   * @param dataSeries     The data contained in a Vector of Doubles.
   * @param searchInterval The indices between which anomalies should be detected. [a, b).
   * @return The indices of all anomalies in the interval and their corresponding wrapper object
   *         with extended results.
   */
  override def detectWithExtendedResults(
      dataSeries: Vector[Double],
      searchInterval: (Int, Int) = (0, Int.MaxValue))
    : Seq[(Int, AnomalyDetectionDataPoint)] = {

    require(dataSeries.nonEmpty, "Provided data series is empty")

    val (start, end) = searchInterval

    require(start < end,
      "Start must be before end")
    require(start >= 0 && end >= 0,
      "The search interval needs to be strictly positive")
    require(start >= seriesPeriodicity * 2,
      "Need at least two full cycles of data to estimate model")

    val numberOfObservationsToForecast =
      if (start >= dataSeries.size) {
        1
      } else {
        math.min(end, dataSeries.size) - start
      }

    val trainingSeries = dataSeries.slice(0, start)
    val optimalParameters = modelSelectionFor(trainingSeries, numberOfObservationsToForecast)
    println("Found optimal parameters for level, trend and seasonality to be " +
      s"${optimalParameters.alpha}, ${optimalParameters.beta} and ${optimalParameters.gamma} " +
      "respectively.")

    // Forecast with estimated parameters
    val modelResults = additiveHoltWinters(
      trainingSeries,
      seriesPeriodicity,
      numberOfObservationsToForecast,
      optimalParameters.alpha,
      optimalParameters.beta,
      optimalParameters.gamma
    )

    val residualsStandardDeviation =
      breeze.stats.stddev(modelResults.residuals.map(math.abs))

    require(modelResults.forecasts.size == numberOfObservationsToForecast)

    val testSeries = dataSeries.drop(start)
    findAnomaliesWithExtendedResults(testSeries, modelResults.forecasts, start, residualsStandardDeviation)
  }
}
