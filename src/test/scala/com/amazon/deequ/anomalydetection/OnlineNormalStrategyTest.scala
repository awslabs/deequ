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

    val (strategy, data, r) = setupDefaultStrategyAndData()


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

  "Online Normal Strategy with Extended Results" should {

    val (strategy, data, r) = setupDefaultStrategyAndData()
    "detect all anomalies if no interval specified" in {
      val strategy = OnlineNormalStrategy(lowerDeviationFactor = Some(3.5),
        upperDeviationFactor = Some(3.5), ignoreStartPercentage = 0.2)
      val anomalyResult = strategy.detectWithExtendedResults(data).filter({case (_, anom) => anom.isAnomaly})

      val expectedResult: Seq[(Int, AnomalyDetectionDataPoint)] = Seq(
        (20, AnomalyDetectionDataPoint(data(20), data(20),
          Threshold(Bound(-14.868489924421404), Bound(14.255383455388895)), isAnomaly = true, 1.0)),
        (21, AnomalyDetectionDataPoint(data(21), data(21),
          Threshold(Bound(-13.6338479733374), Bound(13.02074150430489)), isAnomaly = true, 1.0)),
        (22, AnomalyDetectionDataPoint(data(22), data(22),
          Threshold(Bound(-16.71733585267535), Bound(16.104229383642842)), isAnomaly = true, 1.0)),
        (23, AnomalyDetectionDataPoint(data(23), data(23),
          Threshold(Bound(-17.346915620547467), Bound(16.733809151514958)), isAnomaly = true, 1.0)),
        (24, AnomalyDetectionDataPoint(data(24), data(24),
          Threshold(Bound(-17.496117397890874), Bound(16.883010928858365)), isAnomaly = true, 1.0)),
        (25, AnomalyDetectionDataPoint(data(25), data(25),
          Threshold(Bound(-17.90391150851199), Bound(17.29080503947948)), isAnomaly = true, 1.0)),
        (26, AnomalyDetectionDataPoint(data(26), data(26),
          Threshold(Bound(-17.028892797350824), Bound(16.415786328318315)), isAnomaly = true, 1.0)),
        (27, AnomalyDetectionDataPoint(data(27), data(27),
          Threshold(Bound(-17.720100310354653), Bound(17.106993841322144)), isAnomaly = true, 1.0)),
        (28, AnomalyDetectionDataPoint(data(28), data(28),
          Threshold(Bound(-18.23663168508628), Bound(17.62352521605377)), isAnomaly = true, 1.0)),
        (29, AnomalyDetectionDataPoint(data(29), data(29),
          Threshold(Bound(-19.32641622778204), Bound(18.71330975874953)), isAnomaly = true, 1.0)),
        (30, AnomalyDetectionDataPoint(data(30), data(30),
          Threshold(Bound(-18.96540323993527), Bound(18.35229677090276)), isAnomaly = true, 1.0))
      )
      assert(anomalyResult == expectedResult)
    }

    "only detect anomalies in interval" in {
      val anomalyResult = strategy.detectWithExtendedResults(data, (25, 31)).filter({case (_, anom) => anom.isAnomaly})

      val expectedResult: Seq[(Int, AnomalyDetectionDataPoint)] = Seq(
        (25, AnomalyDetectionDataPoint(data(25), data(25),
         Threshold(Bound(-15.630116599125694), Bound(16.989221350098695)), isAnomaly = true, 1.0)),
        (26, AnomalyDetectionDataPoint(data(26), data(26),
          Threshold(Bound(-14.963376676338362), Bound(16.322481427311363)), isAnomaly = true, 1.0)),
        (27, AnomalyDetectionDataPoint(data(27), data(27),
          Threshold(Bound(-15.131834814393196), Bound(16.490939565366197)), isAnomaly = true, 1.0)),
        (28, AnomalyDetectionDataPoint(data(28), data(28),
          Threshold(Bound(-14.76810451038132), Bound(16.12720926135432)), isAnomaly = true, 1.0)),
        (29, AnomalyDetectionDataPoint(data(29), data(29),
          Threshold(Bound(-15.078145049879462), Bound(16.437249800852463)), isAnomaly = true, 1.0)),
        (30, AnomalyDetectionDataPoint(data(30), data(30),
          Threshold(Bound(-14.540171084298914), Bound(15.899275835271913)), isAnomaly = true, 1.0))
      )
      assert(anomalyResult == expectedResult)
    }

    "ignore lower factor if none is given" in {
      val strategy = OnlineNormalStrategy(lowerDeviationFactor = None,
        upperDeviationFactor = Some(1.5))
      val anomalyResult = strategy.detectWithExtendedResults(data).filter({case (_, anom) => anom.isAnomaly})

      // Anomalies with positive values only
      val expectedResult: Seq[(Int, AnomalyDetectionDataPoint)] = Seq(
        (20, AnomalyDetectionDataPoint(data(20), data(20),
          Threshold(Bound(Double.NegativeInfinity), Bound(5.934276775443095)), isAnomaly = true, 1.0)),
        (22, AnomalyDetectionDataPoint(data(22), data(22),
          Threshold(Bound(Double.NegativeInfinity), Bound(7.979098353666404)), isAnomaly = true, 1.0)),
        (24, AnomalyDetectionDataPoint(data(24), data(24),
          Threshold(Bound(Double.NegativeInfinity), Bound(9.582136909647211)), isAnomaly = true, 1.0)),
        (26, AnomalyDetectionDataPoint(data(26), data(26),
          Threshold(Bound(Double.NegativeInfinity), Bound(10.320400087389258)), isAnomaly = true, 1.0)),
        (28, AnomalyDetectionDataPoint(data(28), data(28),
          Threshold(Bound(Double.NegativeInfinity), Bound(11.113502213504855)), isAnomaly = true, 1.0)),
        (30, AnomalyDetectionDataPoint(data(30), data(30),
          Threshold(Bound(Double.NegativeInfinity), Bound(11.776810456746686)), isAnomaly = true, 1.0))
      )
      assert(anomalyResult == expectedResult)
    }

    "ignore upper factor if none is given" in {
      val strategy = OnlineNormalStrategy(lowerDeviationFactor = Some(1.5),
        upperDeviationFactor = None)
      val anomalyResult = strategy.detectWithExtendedResults(data).filter({case (_, anom) => anom.isAnomaly})

      // Anomalies with negative values only
      val expectedResult: Seq[(Int, AnomalyDetectionDataPoint)] = Seq(
        (21, AnomalyDetectionDataPoint(data(21), data(21),
          Threshold(Bound(-7.855820681098751), Bound(Double.PositiveInfinity)), isAnomaly = true, 1.0)),
        (23, AnomalyDetectionDataPoint(data(23), data(23),
          Threshold(Bound(-10.14631437278386), Bound(Double.PositiveInfinity)), isAnomaly = true, 1.0)),
        (25, AnomalyDetectionDataPoint(data(25), data(25),
          Threshold(Bound(-11.038751996286909), Bound(Double.PositiveInfinity)), isAnomaly = true, 1.0)),
        (27, AnomalyDetectionDataPoint(data(27), data(27),
          Threshold(Bound(-11.359107787232386), Bound(Double.PositiveInfinity)), isAnomaly = true, 1.0)),
        (29, AnomalyDetectionDataPoint(data(29), data(29),
          Threshold(Bound(-12.097995027317015), Bound(Double.PositiveInfinity)), isAnomaly = true, 1.0))
      )
      assert(anomalyResult == expectedResult)
    }

    "work fine with empty input" in {
      val emptySeries = Vector[Double]()
      val anomalyResult = strategy.detectWithExtendedResults(emptySeries).filter({case (_, anom) => anom.isAnomaly})

      assert(anomalyResult == Seq[(Int, AnomalyDetectionDataPoint)]())
    }

    "detect no anomalies if factors are set to max value" in {
      val strategy = OnlineNormalStrategy(Some(Double.MaxValue), Some(Double.MaxValue))
      val anomalyResult = strategy.detectWithExtendedResults(data).filter({case (_, anom) => anom.isAnomaly})

      val expected: List[(Int, AnomalyDetectionDataPoint)] = List()
      assert(anomalyResult == expected)
    }

    "produce error message with correct value and bounds" in {
      val result = strategy.detectWithExtendedResults(data).filter({case (_, anom) => anom.isAnomaly})

      result.foreach { case (_, anom) =>
        val (value, lowerBound, upperBound) =
          AnomalyDetectionTestUtils.firstThreeDoublesFromString(anom.detail.get)

        assert(value === anom.anomalyMetricValue)
        assert(value < lowerBound || value > upperBound)
      }
    }

    "assert anomalies are outside of anomaly bounds" in {
      val result = strategy.detectWithExtendedResults(data).filter({ case (_, anom) => anom.isAnomaly })

      result.foreach { case (_, anom) =>
        val value = anom.anomalyMetricValue
        val upperBound = anom.anomalyThreshold.upperBound.value
        val lowerBound = anom.anomalyThreshold.lowerBound.value

        assert(value < lowerBound || value > upperBound)
      }
    }
  }


  private def setupDefaultStrategyAndData(): (OnlineNormalStrategy, Vector[Double], Random) = {
    val strategy = OnlineNormalStrategy(lowerDeviationFactor = Some(1.5),
      upperDeviationFactor = Some(1.5), ignoreStartPercentage = 0.2)
    val r = new Random(1)

    val dist = (for (_ <- 0 to 50) yield {
      r.nextGaussian()
    }).toArray

    for (i <- 20 to 30)
      dist(i) += i + (i % 2 * -2 * i)

    val data = dist.toVector
    (strategy, data, r)
  }
}
