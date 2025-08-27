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

    "handle null values by creating NullValue bin" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(Some(1.0), None, Some(3.0), None, Some(5.0), Some(7.0), None, Some(9.0)).toDF("values")

      val histogram = HistogramBinned("values", binCount = Some(3))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 4 // 3 numeric bins + 1 NullValue bin
      distribution.bins.size shouldBe 4

      // Check numeric bins (non-null values: 1, 3, 5, 7, 9)
      distribution(0).binStart shouldBe 1.0
      distribution(0).binEnd shouldBe 3.67 +- 0.01
      distribution(0).frequency shouldBe 2 // values 1.0, 3.0
      distribution(0).ratio shouldBe 2.0 / 8.0 +- 0.001

      distribution(1).binStart shouldBe 3.67 +- 0.01
      distribution(1).binEnd shouldBe 6.33 +- 0.01
      distribution(1).frequency shouldBe 1 // value 5.0
      distribution(1).ratio shouldBe 1.0 / 8.0 +- 0.001

      distribution(2).binStart shouldBe 6.33 +- 0.01
      distribution(2).binEnd shouldBe 9.0
      distribution(2).frequency shouldBe 2 // values 7.0, 9.0
      distribution(2).ratio shouldBe 2.0 / 8.0 +- 0.001

      // Check NullValue bin
      distribution(3).binStart shouldBe Double.NegativeInfinity
      distribution(3).binEnd shouldBe Double.PositiveInfinity
      distribution(3).frequency shouldBe 3 // 3 null values
      distribution(3).ratio shouldBe 3.0 / 8.0 +- 0.001
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

    "aggregate sum works as expected with nulls" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        (Some(100.0), Some(50)), // bin 0
        (None, Some(999)), // NullValue bin
        (Some(150.0), Some(75)), // bin 0
        (Some(250.0), None), // bin 1, but null revenue = 0 in sum
        (Some(300.0), Some(125)), // bin 1
        (Some(450.0), Some(200)), // bin 2
        (None, Some(888)) // NullValue bin
      ).toDF("price", "revenue")

      val histogram = HistogramBinned("price", binCount = Some(3), aggregateFunction = Histogram.Sum("revenue"))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 4 // 3 numeric bins + 1 NullValue bin
      distribution.bins.size shouldBe 4

      // Verify sum aggregation with nulls handled
      // Null prices go to NullValue bin
      // Null revenues are treated as 0 in the sum
      distribution(0).frequency shouldBe 125 // 50 + 75 (prices 100, 150)
      distribution(1).frequency shouldBe 125 // 0 + 125 (price 250 has null revenue, price 300 has 125)
      distribution(2).frequency shouldBe 200 // 200 (price 450)

      // NullValue bin gets sum of revenues for null prices
      distribution(3).frequency shouldBe 1887 // 999 + 888 (null prices)
      distribution(3).binStart shouldBe Double.NegativeInfinity
      distribution(3).binEnd shouldBe Double.PositiveInfinity

      // Verify bin ranges (based only on non-null prices: 100, 150, 250, 300, 450)
      distribution(0).binStart shouldBe 100.0
      distribution(0).binEnd shouldBe 216.67 +- 0.01

      distribution(1).binStart shouldBe 216.67 +- 0.01
      distribution(1).binEnd shouldBe 333.33 +- 0.01

      distribution(2).binStart shouldBe 333.33 +- 0.01
      distribution(2).binEnd shouldBe 450.0
    }

    "handle all null data gracefully" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(None: Option[Double], None, None, None).toDF("values")

      val histogram = HistogramBinned("values", binCount = Some(3))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Should have 1 bin for the "NullValue" category (like Histogram does)
      distribution.numberOfBins shouldBe 1
      distribution.bins.size shouldBe 1

      // All nulls become "NullValue" bin - verify it's the special infinity-range bin
      val nullBin = distribution.bins.head
      nullBin.binStart shouldBe Double.NegativeInfinity
      nullBin.binEnd shouldBe Double.PositiveInfinity
      nullBin.frequency shouldBe 4
    }

    "numberOfBins should reflect total bins including empty ones for equal width" in withSparkSession { spark =>
      import spark.implicits._

      // Data with gaps and nulls
      val data = Seq(Some(1.0), None, Some(9.0), None).toDF("values")

      val histogram = HistogramBinned("values", binCount = Some(5)) // Creates 5 equal-width bins
      val result = histogram.calculate(data).value.get

      // numberOfBins should include all bins (regular + null)
      result.numberOfBins shouldBe 6

      // bins collection should include all bins (empty, non-empty, and null)
      result.bins.length shouldBe 6

      // Bin 0: [1.0, 2.6) contains 1.0
      // Bins 1-3: empty.
      // Bin 4: [7.4, 9.0] contains 9.0
      // Bin 5: null bin contains 2 null values
      result.bins(0).frequency shouldBe 1 // Contains 1.0
      result.bins(1).frequency shouldBe 0 // Empty
      result.bins(2).frequency shouldBe 0 // Empty
      result.bins(3).frequency shouldBe 0 // Empty
      result.bins(4).frequency shouldBe 1 // Contains 9.0
      result.bins(5).frequency shouldBe 2 // Contains 2 null values

      // Total values should include nulls for consistency with Histogram
      val totalValues = result.bins.map(_.frequency).sum
      totalValues shouldBe 4 // 2 non-null + 2 null values
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
