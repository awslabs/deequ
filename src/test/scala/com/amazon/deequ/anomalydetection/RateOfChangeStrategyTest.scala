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
      val anomalyResult = strategy.detect(data)
      val expected = for (i <- 20 to 31) yield {
        (i, Anomaly(Option(data(i)), 1.0))
      }
      assert(anomalyResult == expected)
    }
  }
}
