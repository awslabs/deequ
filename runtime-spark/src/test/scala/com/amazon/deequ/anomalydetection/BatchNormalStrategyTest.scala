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

import scala.util.Random

class BatchNormalStrategyTest extends WordSpec with Matchers {

  "Batch Normal Strategy" should {

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
}
