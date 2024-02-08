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

/**
 * The tested class RateOfChangeStrategy is deprecated.
 * This test is to ensure backwards compatibility for deequ checks that still rely on this strategy.
 */
class RateOfChangeStrategyTest extends WordSpec with Matchers {

  "RateOfChange Strategy" should {

    val strategy = RateOfChangeStrategy(Some(-2.0), Some(2.0))
    val data = (for (i <- 0 to 50) yield {
      if (i < 20 || i > 30) {
        1.0
      } else {
        if (i % 2 == 0) i else -i
      }
    }).toVector

    "detect all anomalies if no interval specified" in {
      val anomalyResult = strategy.detect(data).filter({case (_, anom) => anom.isAnomaly})

      val expectedAnomalyThreshold = AnomalyThreshold(Bound(-2.0), Bound(2.0))
      val expectedResult: Seq[(Int, AnomalyDetectionDataPoint)] = Seq(
        (20, AnomalyDetectionDataPoint(20, 19, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (21, AnomalyDetectionDataPoint(-21, -41, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (22, AnomalyDetectionDataPoint(22, 43, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (23, AnomalyDetectionDataPoint(-23, -45, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (24, AnomalyDetectionDataPoint(24, 47, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (25, AnomalyDetectionDataPoint(-25, -49, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (26, AnomalyDetectionDataPoint(26, 51, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (27, AnomalyDetectionDataPoint(-27, -53, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (28, AnomalyDetectionDataPoint(28, 55, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (29, AnomalyDetectionDataPoint(-29, -57, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (30, AnomalyDetectionDataPoint(30, 59, expectedAnomalyThreshold, isAnomaly = true, 1.0)),
        (31, AnomalyDetectionDataPoint(1, -29, expectedAnomalyThreshold, isAnomaly = true, 1.0))
      )
      assert(anomalyResult == expectedResult)
    }
  }
}
