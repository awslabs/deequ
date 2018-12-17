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

class SimpleThresholdStrategyTest extends WordSpec with Matchers {

  "Simple Threshold Strategy" should {

    val strategy = SimpleThresholdStrategy(upperBound = 1.0)
    val data = Vector(-1.0, 2.0, 3.0, 0.5)
    val expected = Seq((1, Anomaly(Option(2.0), 1.0)), (2, Anomaly(Option(3.0), 1.0)))

    "detect values above threshold" in {
      val anomalyResult = strategy.detect(data, (0, 4))

      assert(anomalyResult == expected)
    }

    "detect all values without range specified" in {
      val anomalyResult = strategy.detect(data)

      assert(anomalyResult == expected)
    }

    "work fine with empty input" in {
      val emptySeries = Vector[Double]()
      val anomalyResult = strategy.detect(emptySeries)

      assert(anomalyResult == Seq[(Int, Anomaly)]())
    }

    "work with upper and lower threshold" in {
      val tS = SimpleThresholdStrategy(lowerBound = -0.5, upperBound = 1.0)
      val anomalyResult = tS.detect(data)

      assert(anomalyResult == Seq((0, Anomaly(Option(-1.0), 1.0)),
        (1, Anomaly(Option(2.0), 1.0)), (2, Anomaly(Option(3.0), 1.0))))
    }

    "throw an error when thresholds are not ordered " in {
      intercept[IllegalArgumentException] {
        val ts = SimpleThresholdStrategy(lowerBound = 2.0, upperBound = 1.0)
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
