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

import org.scalatest.{Matchers, WordSpec}
import breeze.stats.meanAndVariance
import scala.util.Random

import scala.math.abs

class OnlineNormalStrategyTest extends WordSpec with Matchers {

  "Online Normal Strategy" should {

    val strategy = OnlineNormalStrategy(lowerDeviationFactor = Some(1.5),
      upperDeviationFactor = Some(1.5), ignoreStartPercentage = 0.2)
    val r = new Random(1)

    val dist = (for (_ <- 0 to 50) yield {
      r.nextGaussian()
    }).toArray

    for (i <- 20 to 30)
      dist(i) += i + (i % 2 * -2 * i)

    val data = dist.toVector

    "detect all anomalies if no interval specified" in {
      val strategy = OnlineNormalStrategy(lowerDeviationFactor = Some(3.5),
        upperDeviationFactor = Some(3.5), ignoreStartPercentage = 0.2)
      val anomalyResult = strategy.detect(data)
      val expected = for (i <- 20 to 30) yield {
        (i, Anomaly(Option(data(i)), 1.0))
      }
      assert(anomalyResult == expected)
    }

    "only detect anomalies in interval" in {
      val anomalyResult = strategy.detect(data, (25, 31))
      val expected = for (i <- 25 to 30) yield {
        (i, Anomaly(Option(data(i)), 1.0))
      }
      assert(anomalyResult == expected)
    }

    "ignore lower factor if none is given" in {
      val strategy = OnlineNormalStrategy(lowerDeviationFactor = None,
        upperDeviationFactor = Some(1.5))
      val anomalyResult = strategy.detect(data)

      // Anomalies with positive values only
      val expected = for (i <- 20 to 30 by 2) yield {
        (i, Anomaly(Option(data(i)), 1.0))
      }
      assert(anomalyResult == expected)
    }

    "ignore upper factor if none is given" in {
      val strategy = OnlineNormalStrategy(lowerDeviationFactor = Some(1.5),
        upperDeviationFactor = None)
      val anomalyResult = strategy.detect(data)

      // Anomalies with negative values only
      val expected = for (i <- 21 to 29 by 2) yield {
        (i, Anomaly(Option(data(i)), 1.0))
      }
      assert(anomalyResult == expected)
    }

    "work fine with empty input" in {
      val emptySeries = Vector[Double]()
      val anomalyResult = strategy.detect(emptySeries)

      assert(anomalyResult == Seq[(Int, Anomaly)]())
    }

    "detect no anomalies if factors are set to max value" in {
      val strategy = OnlineNormalStrategy(Some(Double.MaxValue), Some(Double.MaxValue))
      val anomalyResult = strategy.detect(data)

      val expected: List[(Int, Anomaly)] = List()
      assert(anomalyResult == expected)
    }

    "calculate variance correctly" in {
      val data: Vector[Double] = (for (i <- 1 to 1000) yield {
        r.nextGaussian * (5000.0 / i)
      }).toVector
      val lastPoint = strategy.computeStatsAndAnomalies(data).last

      val breezeResult = meanAndVariance(data)
      val breezeMean = breezeResult.mean
      val breezeStdDev = breezeResult.stdDev

      assert(lastPoint.mean == breezeMean)
      assert(abs(lastPoint.stdDev - breezeStdDev) < breezeStdDev * 0.001)
    }

    "ignores anomalies in calculation if wanted" in {
      val data: Vector[Double] = Vector(1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0)
      val lastPoint = strategy.computeStatsAndAnomalies(data).last

      assert(lastPoint.mean == 1.0)
      assert(lastPoint.stdDev == 0.0)
    }

    "doesn't ignore anomalies in calculation if not wanted" in {
      val strategy = OnlineNormalStrategy(lowerDeviationFactor = Some(1.5),
        upperDeviationFactor = Some(1.5), ignoreStartPercentage = 0.2, ignoreAnomalies = false)
      val data: Vector[Double] = Vector(1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0)
      val lastPoint = strategy.computeStatsAndAnomalies(data).last

      val breezeResult = meanAndVariance(data)
      val breezeMean = breezeResult.mean
      val breezeStdDev = breezeResult.stdDev

      assert(lastPoint.mean == breezeMean)
      assert(abs(lastPoint.stdDev - breezeStdDev) < breezeStdDev * 0.1)
    }

    "throw an error when no factor given" in {
      intercept[IllegalArgumentException] {
        OnlineNormalStrategy(None, None)
      }
    }

    "throw an error when factor is negative" in {
      intercept[IllegalArgumentException] {
        OnlineNormalStrategy(None, Some(-3.0))
      }
      intercept[IllegalArgumentException] {
        OnlineNormalStrategy(Some(-3.0), None)
      }
    }

    "throw an error when percentages are not in range " in {
      intercept[IllegalArgumentException] {
        OnlineNormalStrategy(ignoreStartPercentage = 1.5)
      }
      intercept[IllegalArgumentException] {
        OnlineNormalStrategy(ignoreStartPercentage = -1.0)
      }
    }

    "produce error message with correct value and bounds" in {
      val result = strategy.detect(data)

      result.foreach { case (_, anom) =>
        val (value, lowerBound, upperBound) =
          AnomalyDetectionTestUtils.firstThreeDoublesFromString(anom.detail.get)

        assert(anom.value.isDefined && value === anom.value.get)
        assert(value < lowerBound || value > upperBound)
      }
    }
  }
}
