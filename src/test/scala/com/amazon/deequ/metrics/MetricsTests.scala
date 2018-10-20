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

package com.amazon.deequ.metrics

import com.amazon.deequ.analyzers.DataTypeInstances
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}


class MetricsTests extends WordSpec with Matchers {
  val sampleException = new IllegalArgumentException()
  "Double metric" should {
    "flatten and return itself" in {
      val metric = DoubleMetric(Entity.Column, "metric-name", "instance-name", Success(50))
      assert(metric.flatten() == List(metric))
    }

    "flatten in case of an error" in {
      val metric = DoubleMetric(Entity.Column, "metric-name", "instance-name",
        Failure(sampleException))
      assert(metric.flatten() == List(metric))
    }
  }

  "Histogram metric" should {
    "flatten matched and unmatched" in {
      val distribution = Distribution(
        Map("a" -> DistributionValue(6, 0.6), "b" -> DistributionValue(4, 0.4)), 2)

      val metric = HistogramMetric("instance-name", Success(distribution))

      val expected = Seq(
        DoubleMetric(Entity.Column, "Histogram.bins", "instance-name", Success(2)),
        DoubleMetric(Entity.Column, "Histogram.abs.a", "instance-name", Success(6)),
        DoubleMetric(Entity.Column, "Histogram.abs.b", "instance-name", Success(4)),
        DoubleMetric(Entity.Column, "Histogram.ratio.a", "instance-name", Success(0.6)),
        DoubleMetric(Entity.Column, "Histogram.ratio.b", "instance-name", Success(0.4))
      ).toSet
      assert(metric.flatten().toSet == expected)
    }

    "flatten matched and unmatched in case of an error" in {
      val metric = HistogramMetric("instance-name", Failure(sampleException))

      val expected = Seq(DoubleMetric(Entity.Column, "Histogram.bins", "instance-name",
        Failure(sampleException))).toSet
      assert(metric.flatten().toSet == expected)
    }
  }

}
