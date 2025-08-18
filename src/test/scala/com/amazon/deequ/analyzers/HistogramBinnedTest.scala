/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers.runners.MetricCalculationRuntimeException
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HistogramBinnedTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "HistogramBinned (equal-width edges)" should {
    "create equal-sized bins for integer data with ratio" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30).toDF("values")

      val histogram = HistogramBinned("values", binCount = Some(5))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 5
      distribution.bins.size shouldBe 5

      // Test exact bin ranges and counts
      distribution(0).binStart shouldBe 1.0
      distribution(0).binEnd shouldBe 6.80
      distribution(0).frequency shouldBe 6  // values 1, 2, 3, 4, 5, 6

      distribution(1).binStart shouldBe 6.80
      distribution(1).binEnd shouldBe 12.60
      distribution(1).frequency shouldBe 4  // values 7, 8, 9, 10

      distribution(2).binStart shouldBe 12.60
      distribution(2).binEnd shouldBe 18.40
      distribution(2).frequency shouldBe 1  // value 15

      distribution(3).binStart shouldBe 18.40
      distribution(3).binEnd shouldBe 24.20
      distribution(3).frequency shouldBe 1  // value 20

      distribution(4).binStart shouldBe 24.20
      distribution(4).binEnd shouldBe 30.0
      distribution(4).frequency shouldBe 2  // values 25, 30

      // Verify ratios
      distribution(0).ratio shouldBe 6.0/14.0 +- 0.001  // ~0.429
      distribution(1).ratio shouldBe 4.0/14.0 +- 0.001  // ~0.286
      distribution(2).ratio shouldBe 1.0/14.0 +- 0.001  // ~0.071
      distribution(3).ratio shouldBe 1.0/14.0 +- 0.001  // ~0.071
      distribution(4).ratio shouldBe 2.0/14.0 +- 0.001  // ~0.143

      // Test interval strings
      distribution.getInterval(0) shouldBe "[1.00, 6.80)"
      distribution.getInterval(1) shouldBe "[6.80, 12.60)"
      distribution.getInterval(2) shouldBe "[12.60, 18.40)"
      distribution.getInterval(3) shouldBe "[18.40, 24.20)"
      distribution.getInterval(4) shouldBe "[24.20, 30.00]"
    }

    "create equal-sized bins for integer data" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30).toDF("values")

      val histogram = HistogramBinned(
        "values",
        binCount = Some(5),
        computeFrequenciesAsRatio = false // disable ratio computing
      )
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 5
      distribution.bins.size shouldBe 5

      // Test exact bin ranges and counts
      distribution(0).binStart shouldBe 1.0
      distribution(0).binEnd shouldBe 6.80
      distribution(0).frequency shouldBe 6 // values 1, 2, 3, 4, 5, 6

      distribution(1).binStart shouldBe 6.80
      distribution(1).binEnd shouldBe 12.60
      distribution(1).frequency shouldBe 4 // values 7, 8, 9, 10

      distribution(2).binStart shouldBe 12.60
      distribution(2).binEnd shouldBe 18.40
      distribution(2).frequency shouldBe 1 // value 15

      distribution(3).binStart shouldBe 18.40
      distribution(3).binEnd shouldBe 24.20
      distribution(3).frequency shouldBe 1 // value 20

      distribution(4).binStart shouldBe 24.20
      distribution(4).binEnd shouldBe 30.0
      distribution(4).frequency shouldBe 2 // values 25, 30
      // Test interval strings
      distribution.getInterval(0) shouldBe "[1.00, 6.80)"
      distribution.getInterval(1) shouldBe "[6.80, 12.60)"
      distribution.getInterval(2) shouldBe "[12.60, 18.40)"
      distribution.getInterval(3) shouldBe "[18.40, 24.20)"
      distribution.getInterval(4) shouldBe "[24.20, 30.00]"
    }

    "create equal-sized bins for double data" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1.1, 2.5, 3.7, 4.2, 5.8, 6.3, 7.9, 8.1, 9.4, 10.6).toDF("values")

      val histogram = HistogramBinned("values", binCount = Some(3))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 3
      distribution.bins.size shouldBe 3

      // Test exact bin ranges and counts
      distribution(0).binStart shouldBe 1.1
      distribution(0).binEnd shouldBe 4.27 +- 0.01
      distribution(0).frequency shouldBe 4  // values 1.1, 2.5, 3.7, 4.2

      distribution(1).binStart shouldBe 4.27 +- 0.01
      distribution(1).binEnd shouldBe 7.43 +- 0.01
      distribution(1).frequency shouldBe 2  // values 5.8, 6.3

      distribution(2).binStart shouldBe 7.43 +- 0.01
      distribution(2).binEnd shouldBe 10.6
      distribution(2).frequency shouldBe 4  // values 7.9, 8.1, 9.4, 10.6

      // Test interval strings
      distribution.getInterval(0) shouldBe "[1.10, 4.27)"
      distribution.getInterval(1) shouldBe "[4.27, 7.43)"
      distribution.getInterval(2) shouldBe "[7.43, 10.60]"
    }

    "aggregate sum works as expected" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        (100.0, 50),   // bin 0
        (150.0, 75),   // bin 0
        (250.0, 100),  // bin 1
        (300.0, 125),  // bin 1
        (450.0, 200),  // bin 2
        (500.0, 250)   // bin 2
      ).toDF("price", "revenue")

      val histogram = HistogramBinned("price", binCount = Some(3), aggregateFunction = Histogram.Sum("revenue"))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 3
      distribution.bins.size shouldBe 3

      // Verify sum aggregation (sums of revenue, not counts)
      distribution(0).frequency shouldBe 125  // 50 + 75 (prices 100, 150)
      distribution(1).frequency shouldBe 225  // 100 + 125 (prices 250, 300)
      distribution(2).frequency shouldBe 450  // 200 + 250 (prices 450, 500)

      // Verify bin ranges
      distribution(0).binStart shouldBe 100.0
      distribution(0).binEnd shouldBe 233.33 +- 0.01

      distribution(1).binStart shouldBe 233.33 +- 0.01
      distribution(1).binEnd shouldBe 366.67 +- 0.01

      distribution(2).binStart shouldBe 366.67 +- 0.01
      distribution(2).binEnd shouldBe 500.0
    }
  }

  "HistogramBinned (custom edges)" should {
    "throw UnsupportedOperationException for custom edges (not yet implemented)" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1, 2, 3, 4, 5).toDF("values")
      val customEdges = Array(1.0, 2.0, 3.0, 4.0, 5.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data)

      result.value.isFailure shouldBe true
      result.value.failed.get shouldBe a[MetricCalculationRuntimeException]
      result.value.failed.get.getCause shouldBe a[UnsupportedOperationException]
      result.value.failed.get.getCause.getMessage shouldBe "Custom edges not yet implemented"
    }
  }

  "HistogramBinned parameter validation" should {
    "throw IllegalArgumentException when neither binCount nor customEdges is provided" in {
      val exception = intercept[IllegalArgumentException] {
        HistogramBinned("values")
      }
      exception.getMessage should include("Must specify either binCount (equal-width) or customEdges (custom)")
    }

    "throw IllegalArgumentException when both binCount and customEdges are provided" in {
      val exception = intercept[IllegalArgumentException] {
        HistogramBinned("values", binCount = Some(5), customEdges = Some(Array(1.0, 2.0, 3.0)))
      }
      exception.getMessage should include("Must specify either binCount (equal-width) or customEdges (custom)")
    }
  }
}
