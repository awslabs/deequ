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

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, PrivateMethodTester, WordSpec}


class AnomalyDetectorTest extends WordSpec with Matchers with MockFactory with PrivateMethodTester {
  private val fakeAnomalyDetector = stub[AnomalyDetectionStrategy]

  val aD = AnomalyDetector(fakeAnomalyDetector)
  val data = Seq((0L, -1.0), (1L, 2.0), (2L, 3.0), (3L, 0.5)).map { case (t, v) =>
    DataPoint[Double](t, Option(v))
  }

  "Anomaly Detector" should {

    "ignore missing values" in {
      val data = Seq(DataPoint[Double](0L, Option(1.0)), DataPoint[Double](1L, Option(2.0)),
        DataPoint[Double](2L, None), DataPoint[Double](3L, Option(1.0)))

      (fakeAnomalyDetector.detect _ when(Vector(1.0, 2.0, 1.0), (0, 3)))
        .returns(Seq((1, Anomaly(Option(2.0), 1.0))))

      val anomalyResult = aD.detectAnomaliesInHistory(data, (0L, 4L))

      assert(anomalyResult == DetectionResult(Seq((1L, Anomaly(Option(2.0), 1.0)))))
    }

    "only detect values in range" in {
      (fakeAnomalyDetector.detect _ when(Vector(-1.0, 2.0, 3.0, 0.5), (2, 4)))
        .returns(Seq((2, Anomaly(Option(3.0), 1.0))))

      val anomalyResult = aD.detectAnomaliesInHistory(data, (2L, 4L))

      assert(anomalyResult == DetectionResult(Seq((2L, Anomaly(Option(3.0), 1.0)))))
    }

    "throw an error when intervals are not ordered" in {
      intercept[IllegalArgumentException] {
        aD.detectAnomaliesInHistory(data, (4, 2))
      }
    }

    "treat ordered values with time gaps correctly" in {
      val data = (for (i <- 1 to 10) yield {
        (i.toLong * 200L) -> 5.0
      }).map { case (t, v) =>
        DataPoint[Double](t, Option(v))
      }

      (fakeAnomalyDetector.detect _ when(data.map(_.metricValue.get).toVector, (0, 2)))
        .returns (Seq((0, Anomaly(Option(5.0), 1.0)), (1, Anomaly(Option(5.0), 1.0))))

      val anomalyResult = aD.detectAnomaliesInHistory(data, (200L, 401L))

      assert(anomalyResult == DetectionResult(Seq((200L, Anomaly(Option(5.0), 1.0)),
        (400L, Anomaly(Option(5.0), 1.0)))))
    }

    "treat unordered values with time gaps correctly" in {
      val data = Seq((10L, -1.0), (25L, 2.0), (11L, 3.0), (0L, 0.5)).map { case (t, v) =>
        DataPoint[Double](t, Option(v))
      }
      val tS = AnomalyDetector(SimpleThresholdStrategy(lowerBound = -0.5, upperBound = 1.0))

      (fakeAnomalyDetector.detect _ when(Vector(0.5, -1.0, 3.0, 2.0), (0, 4)))
        .returns(Seq((1, Anomaly(Option(-1.0), 1.0)), (2, Anomaly(Option(3.0), 1.0)),
          (3, Anomaly(Option(2.0), 1.0))))

      val anomalyResult = aD.detectAnomaliesInHistory(data)

      assert(anomalyResult == DetectionResult(Seq((10L, Anomaly(Option(-1.0), 1.0)),
        (11L, Anomaly(Option(3.0), 1.0)), (25L, Anomaly(Option(2.0), 1.0)))))
    }

    "treat unordered values without time gaps correctly" in {
      val data = Seq((1L, -1.0), (3L, 2.0), (2L, 3.0), (0L, 0.5)).map { case (t, v) =>
        DataPoint[Double](t, Option(v))
      }

      (fakeAnomalyDetector.detect _ when(Vector(0.5, -1.0, 3.0, 2.0), (0, 4)))
        .returns(Seq((1, Anomaly(Option(-1.0), 1.0)), (2, Anomaly(Option(3.0), 1.0)),
          (3, Anomaly(Option(2.0), 1.0))))

      val anomalyResult = aD.detectAnomaliesInHistory(data)

      assert(anomalyResult == DetectionResult(Seq((1L, Anomaly(Option(-1.0), 1.0)),
        (2L, Anomaly(Option(3.0), 1.0)), (3L, Anomaly(Option(2.0), 1.0)))))
    }

  }
}
