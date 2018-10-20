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

import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}

class HistoryUtilsTest extends WordSpec with Matchers {

  "History Utils" should {
    val sampleException = new IllegalArgumentException()

    val noneMetric = None
    val metricWithNoValue = Some(DoubleMetric(Entity.Column, "metric-name", "instance-name",
      Failure(sampleException)))
    val metricWithValue = Some(DoubleMetric(Entity.Column, "metric-name", "instance-name",
      Success(50)))

    "extract optinal metric value" in {
      assert(HistoryUtils.extractMetricValue[Double](noneMetric).isEmpty)
      assert(HistoryUtils.extractMetricValue[Double](metricWithNoValue).isEmpty)
      assert(HistoryUtils.extractMetricValue[Double](metricWithValue).contains(50))

    }
    "extract optinal metric values" in {
      val metrics = Seq(0L -> noneMetric, 1L -> metricWithNoValue, 2L -> metricWithValue)
      assert(HistoryUtils.extractMetricValues[Double](metrics) == Seq(DataPoint[Double](0L, None),
        DataPoint[Double](1L, None), DataPoint[Double](2, Some(50))))
    }
  }
}

