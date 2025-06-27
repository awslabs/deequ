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

import org.scalatest.Matchers
import org.scalatest.WordSpec

import scala.util.Random

class BatchNormalStrategyTest extends WordSpec with Matchers {

  "Batch Normal Strategy" should {

    val (strategy, data) = setupDefaultStrategyAndData()

    "only detect anomalies in interval" in {
      val anomalyResult = strategy.detect(data, (25, 50))
      val expected = for (i <- 25 to 30) yield {
        (i, Anomaly(Option(data(i)), 1.0))
      }
      assert(anomalyResult == expected)
    }

    "ignore lower factor if none is given" in {
      val strategy = BatchNormalStrategy(None, Some(1.0))
      val anomalyResult = strategy.detect(data, (20, 31))

      // Anomalies with positive values only
      val expected = for (i <- 20 to 30 by 2) yield {
        (i, Anomaly(Option(data(i)), 1.0))
      }
      assert(anomalyResult == expected)
    }

    "ignore upper factor if none is given" in {
      val strategy = BatchNormalStrategy(Some(1.0), None)
      val anomalyResult = strategy.detect(data, (10, 30))

      // Anomalies with negative values only
      val expected = for (i <- 21 to 29 by 2) yield {
        (i, Anomaly(Option(data(i)), 1.0))
      }
      assert(anomalyResult == expected)
    }

    "ignore values in interval for mean/ stdDev if specified" in {
      val data = Vector(1.0, 1.0, 1.0, 1000.0, 500.0, 1.0)
      val strategy = BatchNormalStrategy(Some(3.0), Some(3.0))
      val anomalyResult = strategy.detect(data, (3, 5))
      val expected = Seq((3, Anomaly(Option(data(3)), 1.0)), (4, Anomaly(Option(data(4)), 1.0)))

      assert(anomalyResult == expected)
    }

    "throw an exception when trying to exclude all data points from calculation" in {
      val strategy = BatchNormalStrategy()
      intercept[IllegalArgumentException] {
        strategy.detect(data)
      }
    }


    "detect no anomalies if factors are set to max value" in {
      val strategy = BatchNormalStrategy(Some(Double.MaxValue), Some(Double.MaxValue))
      val anomalyResult = strategy.detect(data, (30, 51))

      val expected: List[(Int, Anomaly)] = List()
      assert(anomalyResult == expected)
    }

    "throw an error when factor is negative" in {
      intercept[IllegalArgumentException] {
        BatchNormalStrategy(None, Some(-3.0))
      }
      intercept[IllegalArgumentException] {
        BatchNormalStrategy(Some(-3.0), None)
      }
    }

    "throw an error when no factor given" in {
      intercept[IllegalArgumentException] {
        BatchNormalStrategy(None, None)
      }
    }

    "produce error message with correct value and bounds" in {
      val result = strategy.detect(data, (25, 50))

      result.foreach { case (_, anom) =>
        val (value, lowerBound, upperBound) =
          AnomalyDetectionTestUtils.firstThreeDoublesFromString(anom.detail.get)

        assert(anom.value.isDefined && value === anom.value.get)
        assert(value < lowerBound || value > upperBound)
      }
    }
  }

  "Batch Normal Strategy using Extended Results " should {

    val (strategy, data) = setupDefaultStrategyAndData()

    "only detect anomalies in interval" in {
      val anomalyResult =
        strategy.detectWithExtendedResults(data, (25, 50)).filter({ case (_, anom) => anom.isAnomaly })

      val expectedAnomalyCheckRange = BoundedRange(Bound(-9.280850004177061, inclusive = true),
        Bound(10.639954755150061, inclusive = true))
      val expectedResult: Seq[(Int, AnomalyDetectionDataPoint)] = Seq(
        (25, AnomalyDetectionDataPoint(data(25), data(25), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (26, AnomalyDetectionDataPoint(data(26), data(26), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (27, AnomalyDetectionDataPoint(data(27), data(27), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (28, AnomalyDetectionDataPoint(data(28), data(28), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (29, AnomalyDetectionDataPoint(data(29), data(29), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (30, AnomalyDetectionDataPoint(data(30), data(30), expectedAnomalyCheckRange, isAnomaly = true, 1.0))
      )
      assert(anomalyResult == expectedResult)
    }

    "ignore lower factor if none is given" in {
      val strategy = BatchNormalStrategy(None, Some(1.0))
      val anomalyResult =
        strategy.detectWithExtendedResults(data, (20, 31)).filter({ case (_, anom) => anom.isAnomaly })

      val expectedAnomalyCheckRange = BoundedRange(Bound(Double.NegativeInfinity, inclusive = true),
        Bound(0.7781496015857838, inclusive = true))
      // Anomalies with positive values only
      val expectedResult: Seq[(Int, AnomalyDetectionDataPoint)] = Seq(
        (20, AnomalyDetectionDataPoint(data(20), data(20), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (22, AnomalyDetectionDataPoint(data(22), data(22), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (24, AnomalyDetectionDataPoint(data(24), data(24), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (26, AnomalyDetectionDataPoint(data(26), data(26), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (28, AnomalyDetectionDataPoint(data(28), data(28), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (30, AnomalyDetectionDataPoint(data(30), data(30), expectedAnomalyCheckRange, isAnomaly = true, 1.0))
      )
      assert(anomalyResult == expectedResult)
    }

    "ignore upper factor if none is given" in {
      val strategy = BatchNormalStrategy(Some(1.0), None)
      val anomalyResult =
        strategy.detectWithExtendedResults(data, (10, 30)).filter({ case (_, anom) => anom.isAnomaly })
      val expectedAnomalyCheckRange = BoundedRange(Bound(-5.063730045618394, inclusive = true),
        Bound(Double.PositiveInfinity, inclusive = true))

      // Anomalies with negative values only
      val expectedResult: Seq[(Int, AnomalyDetectionDataPoint)] = Seq(
        (21, AnomalyDetectionDataPoint(data(21), data(21), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (23, AnomalyDetectionDataPoint(data(23), data(23), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (25, AnomalyDetectionDataPoint(data(25), data(25), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (27, AnomalyDetectionDataPoint(data(27), data(27), expectedAnomalyCheckRange, isAnomaly = true, 1.0)),
        (29, AnomalyDetectionDataPoint(data(29), data(29), expectedAnomalyCheckRange, isAnomaly = true, 1.0))
      )
      assert(anomalyResult == expectedResult)
    }

    "ignore values in interval for mean/ stdDev if specified" in {
      val data = Vector(1.0, 1.0, 1.0, 1000.0, 500.0, 1.0)
      val strategy = BatchNormalStrategy(Some(3.0), Some(3.0))
      val anomalyResult =
        strategy.detectWithExtendedResults(data, (3, 5)).filter({ case (_, anom) => anom.isAnomaly })

      val expectedResult: Seq[(Int, AnomalyDetectionDataPoint)] = Seq(
        (3, AnomalyDetectionDataPoint(1000, 1000, BoundedRange(Bound(1.0, inclusive = true),
          Bound(1.0, inclusive = true)), isAnomaly = true, 1.0)),
        (4, AnomalyDetectionDataPoint(500, 500, BoundedRange(Bound(1.0, inclusive = true),
          Bound(1.0, inclusive = true)), isAnomaly = true, 1.0))
      )
      assert(anomalyResult == expectedResult)
    }

    "throw an exception when trying to exclude all data points from calculation" in {
      val strategy = BatchNormalStrategy()
      intercept[IllegalArgumentException] {
        strategy.detectWithExtendedResults(data).filter({ case (_, anom) => anom.isAnomaly })
      }
    }
    "detect no anomalies if factors are set to max value" in {
      val strategy = BatchNormalStrategy(Some(Double.MaxValue), Some(Double.MaxValue))
      val anomalyResult =
        strategy.detectWithExtendedResults(data, (30, 51)).filter({ case (_, anom) => anom.isAnomaly })

      val expected: List[(Int, AnomalyDetectionDataPoint)] = List()
      assert(anomalyResult == expected)
    }

    "produce error message with correct value and bounds" in {
      val result = strategy.detectWithExtendedResults(data, (25, 50)).filter({ case (_, anom) => anom.isAnomaly })

      result.foreach { case (_, anom) =>
        val (value, lowerBound, upperBound) =
          AnomalyDetectionTestUtils.firstThreeDoublesFromString(anom.detail.get)

        assert(value === anom.anomalyMetricValue)
        assert(value < lowerBound || value > upperBound)
      }
    }

    "assert anomalies are outside of anomaly bounds" in {
      val result = strategy.detectWithExtendedResults(data, (25, 50)).filter({ case (_, anom) => anom.isAnomaly })

      result.foreach { case (_, anom) =>
        val value = anom.anomalyMetricValue
        val upperBound = anom.anomalyCheckRange.upperBound.value
        val lowerBound = anom.anomalyCheckRange.lowerBound.value

        assert(value < lowerBound || value > upperBound)
      }


    }
  }

  private def setupDefaultStrategyAndData(): (BatchNormalStrategy, Vector[Double]) = {
    val strategy =
      BatchNormalStrategy(lowerDeviationFactor = Some(1.0), upperDeviationFactor = Some(1.0))

    val r = new Random(1)
    val dist = (for (_ <- 0 to 49) yield {
      r.nextGaussian()
    }).toArray

    for (i <- 20 to 30) {
      dist(i) += i + (i % 2 * -2 * i)
    }

    val data = dist.toVector
    (strategy, data)
  }
}
