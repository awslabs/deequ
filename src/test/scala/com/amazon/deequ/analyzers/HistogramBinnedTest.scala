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
import com.amazon.deequ.analyzers.runners.IllegalAnalyzerParameterException
import com.amazon.deequ.analyzers.runners.MetricCalculationRuntimeException
import com.amazon.deequ.utils.FixtureSupport
import scala.util.Failure
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
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

      distribution.numberOfBins shouldBe 3 // only data bins
      distribution.bins.size shouldBe 3

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

      // Null count is separate
      distribution.nullCount shouldBe 3
    }

    "emit nullCount in flatten() when nulls exist" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(Some(1.0), None, Some(5.0), None).toDF("values")
      val histogram = HistogramBinned("values", binCount = Some(3))
      val metric = histogram.calculate(data)

      val flattened = metric.flatten()
      flattened.exists(m => m.name == "HistogramBinned.nullCount" && m.value.get == 2.0) shouldBe true
    }

    "not emit nullCount in flatten() when no nulls" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1.0, 2.0, 3.0).toDF("values")
      val histogram = HistogramBinned("values", binCount = Some(3))
      val metric = histogram.calculate(data)

      val flattened = metric.flatten()
      flattened.exists(_.name == "HistogramBinned.nullCount") shouldBe false
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

      distribution.numberOfBins shouldBe 3 // only data bins
      distribution.bins.size shouldBe 3

      // Verify sum aggregation with nulls handled
      // Null revenues are treated as 0 in the sum
      distribution(0).frequency shouldBe 125 // 50 + 75 (prices 100, 150)
      distribution(1).frequency shouldBe 125 // 0 + 125 (price 250 has null revenue, price 300 has 125)
      distribution(2).frequency shouldBe 200 // 200 (price 450)

      // NullValue tracked separately
      distribution.nullCount shouldBe 1887 // 999 + 888 (null prices)

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

      // No data bins when all values are null
      distribution.numberOfBins shouldBe 0
      distribution.bins.size shouldBe 0

      // All nulls tracked in nullCount
      distribution.nullCount shouldBe 4
    }

    "numberOfBins should reflect total bins including empty ones for equal width" in withSparkSession { spark =>
      import spark.implicits._

      // Data with gaps and nulls
      val data = Seq(Some(1.0), None, Some(9.0), None).toDF("values")

      val histogram = HistogramBinned("values", binCount = Some(5)) // Creates 5 equal-width bins
      val result = histogram.calculate(data).value.get

      // numberOfBins is only data bins
      result.numberOfBins shouldBe 5

      // bins collection has only data bins
      result.bins.length shouldBe 5

      // Bin 0: [1.0, 2.6) contains 1.0
      // Bins 1-3: empty
      // Bin 4: [7.4, 9.0] contains 9.0
      result.bins(0).frequency shouldBe 1 // Contains 1.0
      result.bins(1).frequency shouldBe 0 // Empty
      result.bins(2).frequency shouldBe 0 // Empty
      result.bins(3).frequency shouldBe 0 // Empty
      result.bins(4).frequency shouldBe 1 // Contains 9.0

      // Nulls tracked separately
      result.nullCount shouldBe 2
    }
  }

  "HistogramBinned (custom edges)" should {
    "create bins with custom edges for integer data" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30).toDF("values")
      val customEdges = Array(0.0, 5.0, 10.0, 20.0, 35.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 4
      distribution.bins.size shouldBe 4

      // Custom edges follow left-inclusive, right-exclusive convention [a, b) except last bin [a, b]
      // Bin 0: [0.0, 5.0) - includes 0.0, excludes 5.0 -> values 1, 2, 3, 4
      // Bin 1: [5.0, 10.0) - includes 5.0, excludes 10.0 -> values 5, 6, 7, 8, 9 (note: 5 is included, 10 is excluded)
      // Bin 2: [10.0, 20.0) - includes 10.0, excludes 20.0 -> values 10, 15 (note: 10 is included here)
      // Bin 3: [20.0, 35.0] - includes both bounds -> values 20, 25, 30

      // Test exact bin ranges and counts
      distribution(0).binStart shouldBe 0.0
      distribution(0).binEnd shouldBe 5.0
      distribution(0).frequency shouldBe 4  // values 1, 2, 3, 4

      distribution(1).binStart shouldBe 5.0
      distribution(1).binEnd shouldBe 10.0
      distribution(1).frequency shouldBe 5  // values 5, 6, 7, 8, 9 (5 included, 10 excluded)

      distribution(2).binStart shouldBe 10.0
      distribution(2).binEnd shouldBe 20.0
      distribution(2).frequency shouldBe 2  // values 10, 15 (10 included here)

      distribution(3).binStart shouldBe 20.0
      distribution(3).binEnd shouldBe 35.0
      distribution(3).frequency shouldBe 3  // values 20, 25, 30

      // Test interval strings
      distribution.getInterval(0) shouldBe "[0.00, 5.00)"
      distribution.getInterval(1) shouldBe "[5.00, 10.00)"
      distribution.getInterval(2) shouldBe "[10.00, 20.00)"
      distribution.getInterval(3) shouldBe "[20.00, 35.00]"
    }

    "create bins with custom edges for double data" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1.1, 2.5, 3.7, 4.0, 4.2, 5.8, 6.3, 7.0, 7.9, 8.1, 9.4, 10.6).toDF("values")
      val customEdges = Array(1.0, 4.0, 7.0, 11.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 3
      distribution.bins.size shouldBe 3

      // Custom edges follow left-inclusive, right-exclusive convention [a, b) except last bin [a, b]
      // Bin 0: [1.0, 4.0) - includes 1.0, excludes 4.0 -> values 1.1, 2.5, 3.7 (note: 4.0 is excluded)
      // Bin 1: [4.0, 7.0) - includes 4.0, excludes 7.0 -> values 4.0, 4.2, 5.8, 6.3 (note: 4.0 included, 7.0 excluded)
      // Bin 2: [7.0, 11.0] - includes both bounds -> values 7.0, 7.9, 8.1, 9.4, 10.6 (note: 7.0 included here)

      // Test exact bin ranges and counts
      distribution(0).binStart shouldBe 1.0
      distribution(0).binEnd shouldBe 4.0
      distribution(0).frequency shouldBe 3  // values 1.1, 2.5, 3.7 (4.0 excluded)

      distribution(1).binStart shouldBe 4.0
      distribution(1).binEnd shouldBe 7.0
      distribution(1).frequency shouldBe 4  // values 4.0, 4.2, 5.8, 6.3 (4.0 included, 7.0 excluded)

      distribution(2).binStart shouldBe 7.0
      distribution(2).binEnd shouldBe 11.0
      distribution(2).frequency shouldBe 5  // values 7.0, 7.9, 8.1, 9.4, 10.6 (7.0 included here)

      // Test interval strings
      distribution.getInterval(0) shouldBe "[1.00, 4.00)"
      distribution.getInterval(1) shouldBe "[4.00, 7.00)"
      distribution.getInterval(2) shouldBe "[7.00, 11.00]"
    }

    "handle floating point precision at boundaries" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(9.999999999, 10.0, 10.000000001).toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data).value.get
      val distribution = result.bins

      distribution.length shouldBe 2

      // 9.999999999 should go in bin 0: [0.0, 10.0)
      // 10.0 should go in bin 1: [10.0, 20.0]
      // 10.000000001 should go in bin 1: [10.0, 20.0]
      distribution(0).frequency shouldBe 1  // 9.999999999
      distribution(1).frequency shouldBe 2  // 10.0, 10.000000001
    }

    "handle null values with custom edges" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(Some(1.0), None, Some(3.0), None, Some(5.0), Some(7.0), None, Some(9.0)).toDF("values")
      val customEdges = Array(0.0, 4.0, 8.0, 10.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 3 // only data bins
      distribution.bins.size shouldBe 3

      // Check numeric bins (non-null values: 1, 3, 5, 7, 9)
      distribution(0).binStart shouldBe 0.0
      distribution(0).binEnd shouldBe 4.0
      distribution(0).frequency shouldBe 2 // values 1.0, 3.0

      distribution(1).binStart shouldBe 4.0
      distribution(1).binEnd shouldBe 8.0
      distribution(1).frequency shouldBe 2 // values 5.0, 7.0

      distribution(2).binStart shouldBe 8.0
      distribution(2).binEnd shouldBe 10.0
      distribution(2).frequency shouldBe 1 // value 9.0

      // Null count is separate
      distribution.nullCount shouldBe 3
    }

    "handle duplicate values at bin boundaries correctly" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(5.0, 5.0, 5.0, 10.0, 10.0, 15.0).toDF("values")
      val customEdges = Array(0.0, 5.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 3
      distribution.bins.size shouldBe 3

      // Multiple 5.0 values should all go to bin 1 [5.0, 10.0)
      // Multiple 10.0 values should all go to bin 2 [10.0, 20.0]
      distribution(0).frequency shouldBe 0  // [0.0, 5.0) - empty
      distribution(1).frequency shouldBe 3  // [5.0, 10.0) - three 5.0 values
      distribution(2).frequency shouldBe 3  // [10.0, 20.0] - two 10.0 values + one 15.0
    }

    "work with single bin (two edges)" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1, 2, 3, 4, 5).toDF("values")
      val customEdges = Array(0.0, 10.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 1
      distribution.bins.size shouldBe 1

      // All values should go into the single bin [0.0, 10.0]
      distribution(0).binStart shouldBe 0.0
      distribution(0).binEnd shouldBe 10.0
      distribution(0).frequency shouldBe 5
      distribution.getInterval(0) shouldBe "[0.00, 10.00]"
    }

    "handle negative edges correctly" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(-10, -5, 0, 5, 10).toDF("values")
      val customEdges = Array(-15.0, -2.0, 3.0, 12.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 3
      distribution.bins.size shouldBe 3

      // Bin 0: [-15.0, -2.0) should contain -10, -5
      // Bin 1: [-2.0, 3.0) should contain 0
      // Bin 2: [3.0, 12.0] should contain 5, 10
      distribution(0).frequency shouldBe 2  // -10, -5
      distribution(1).frequency shouldBe 1  // 0
      distribution(2).frequency shouldBe 2  // 5, 10
    }

    "handle boundary values correctly" in withSparkSession { spark =>
      import spark.implicits._

      // Test values exactly on edges - critical for binary search correctness
      val data = Seq(0.0, 5.0, 10.0, 15.0, 20.0).toDF("values")
      val customEdges = Array(0.0, 5.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data).value.get
      val distribution = result.bins

      distribution.length shouldBe 3

      // Bin 0: [0.0, 5.0) should contain 0.0 only
      distribution(0).frequency shouldBe 1
      distribution(0).binStart shouldBe 0.0
      distribution(0).binEnd shouldBe 5.0

      // Bin 1: [5.0, 10.0) should contain 5.0 only
      distribution(1).frequency shouldBe 1
      distribution(1).binStart shouldBe 5.0
      distribution(1).binEnd shouldBe 10.0

      // Bin 2: [10.0, 20.0] should contain 10.0, 15.0, 20.0 (last bin includes upper bound)
      distribution(2).frequency shouldBe 3
      distribution(2).binStart shouldBe 10.0
      distribution(2).binEnd shouldBe 20.0
    }

    "handle gaps with empty bins" in withSparkSession { spark =>
      import spark.implicits._

      // Data that skips middle bins - tests binary search with sparse data
      val data = Seq(1.0, 19.0).toDF("values")
      val customEdges = Array(0.0, 5.0, 10.0, 15.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data).value.get
      val distribution = result.bins

      distribution.length shouldBe 4

      // Bin 0: [0.0, 5.0) should contain 1.0
      distribution(0).frequency shouldBe 1

      // Bin 1: [5.0, 10.0) should be empty
      distribution(1).frequency shouldBe 0

      // Bin 2: [10.0, 15.0) should be empty
      distribution(2).frequency shouldBe 0

      // Bin 3: [15.0, 20.0] should contain 19.0
      distribution(3).frequency shouldBe 1
    }

    "handle unsorted custom edges by sorting them" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("values")
      val customEdges = Array(10.0, 0.0, 5.0) // unsorted

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 2
      distribution.bins.size shouldBe 2

      // Should be sorted to [0.0, 5.0, 10.0]
      distribution(0).binStart shouldBe 0.0
      distribution(0).binEnd shouldBe 5.0
      distribution(0).frequency shouldBe 4  // values 1, 2, 3, 4

      distribution(1).binStart shouldBe 5.0
      distribution(1).binEnd shouldBe 10.0
      distribution(1).frequency shouldBe 6  // values 5, 6, 7, 8, 9, 10
    }

    "work with Sum aggregation and custom edges" in withSparkSession { spark =>
      import spark.implicits._

      // Tax bracket example: income ranges and total tax collected per bracket
      val data = Seq(
        (25000.0, 2500), // Low income bracket
        (35000.0, 4200), // Low income bracket
        (45000.0, 6750), // Middle income bracket
        (75000.0, 15000), // Middle income bracket
        (120000.0, 28800), // High income bracket
        (200000.0, 54000) // High income bracket
      ).toDF("income", "tax_paid")

      // Tax brackets: 0-40k, 40k-100k, 100k+
      val customEdges = Array(0.0, 40000.0, 100000.0, 300000.0)

      val histogram = HistogramBinned(
        "income",
        customEdges = Some(customEdges),
        aggregateFunction = Histogram.Sum("tax_paid")
      )
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 3
      distribution.bins.size shouldBe 3

      // Verify sum aggregation - total tax collected per income bracket
      distribution(0).frequency shouldBe 6700 // 2500 + 4200 (incomes 25k, 35k)
      distribution(1).frequency shouldBe 21750 // 6750 + 15000 (incomes 45k, 75k)
      distribution(2).frequency shouldBe 82800 // 28800 + 54000 (incomes 120k, 200k)

      // Verify bin ranges represent tax brackets
      distribution(0).binStart shouldBe 0.0
      distribution(0).binEnd shouldBe 40000.0
      distribution(1).binStart shouldBe 40000.0
      distribution(1).binEnd shouldBe 100000.0
      distribution(2).binStart shouldBe 100000.0
      distribution(2).binEnd shouldBe 300000.0
    }
  }

  "HistogramBinned (overflow bins)" should {
    "add overflow bins to custom edges" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1.0, 5.0, 15.0, 25.0).toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 4

      distribution(0).binStart shouldBe Double.NegativeInfinity
      distribution(0).binEnd shouldBe 0.0
      distribution(0).frequency shouldBe 0

      distribution(1).binStart shouldBe 0.0
      distribution(1).binEnd shouldBe 10.0
      distribution(1).frequency shouldBe 2

      distribution(2).binStart shouldBe 10.0
      distribution(2).binEnd shouldBe 20.0
      distribution(2).frequency shouldBe 1

      distribution(3).binStart shouldBe 20.0
      distribution(3).binEnd shouldBe Double.PositiveInfinity
      distribution(3).frequency shouldBe 1
    }

    "add overflow bins to auto-computed edges" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1.0, 2.0, 3.0, 4.0, 5.0).toDF("values")

      // binCount = 5 with overflow: 3 interior + 2 overflow = 5 total
      val histogram = HistogramBinned("values", binCount = Some(5), includeOverflowBins = true)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 5
      distribution(0).binStart shouldBe Double.NegativeInfinity
      distribution(0).binEnd shouldBe 1.0
      distribution(0).frequency shouldBe 0  // nothing below min

      // Max value (5.0) stays in last interior bin (inclusive)
      // Right overflow is empty on first run
      distribution(4).binStart shouldBe 5.0
      distribution(4).binEnd shouldBe Double.PositiveInfinity
      distribution(4).frequency shouldBe 0
    }

    "not duplicate infinity edges if already present" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(1.0, 5.0, 20.0, 25.0).toDF("values")
      val customEdges = Array(Double.NegativeInfinity, 0.0, 10.0, 20.0, Double.PositiveInfinity)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 4
      distribution(0).binStart shouldBe Double.NegativeInfinity
      distribution(3).binEnd shouldBe Double.PositiveInfinity

      // 20.0 stays in last interior bin [10, 20] not overflow [20, Inf)
      distribution(2).frequency shouldBe 1  // 20.0
      distribution(3).frequency shouldBe 1  // 25.0 in overflow
    }

    "separate nulls from overflow" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(Some(-5.0), None, Some(5.0), Some(25.0), None).toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // 4 data/overflow bins, nulls separate
      distribution.numberOfBins shouldBe 4

      distribution(0).binStart shouldBe Double.NegativeInfinity
      distribution(0).binEnd shouldBe 0.0
      distribution(0).frequency shouldBe 1

      distribution(3).binStart shouldBe 20.0
      distribution(3).binEnd shouldBe Double.PositiveInfinity
      distribution(3).frequency shouldBe 1

      // Nulls in separate field
      distribution.nullCount shouldBe 2
    }

    "handle unsorted custom edges with overflow" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(-1.0, 5.0, 15.0).toDF("values")
      val customEdges = Array(10.0, 0.0) // unsorted

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Sorted to [0.0, 10.0], then overflow added: [-Inf, 0.0, 10.0, Inf]
      distribution.numberOfBins shouldBe 3
      distribution(0).binStart shouldBe Double.NegativeInfinity
      distribution(0).binEnd shouldBe 0.0
      distribution(0).frequency shouldBe 1  // -1.0

      distribution(1).binStart shouldBe 0.0
      distribution(1).binEnd shouldBe 10.0
      distribution(1).frequency shouldBe 1  // 5.0

      distribution(2).binStart shouldBe 10.0
      distribution(2).binEnd shouldBe Double.PositiveInfinity
      distribution(2).frequency shouldBe 1  // 15.0
    }

    "format overflow intervals correctly" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(5.0).toDF("values")
      val customEdges = Array(0.0, 10.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data).value.get

      result.getInterval(0) shouldBe "(-Inf, 0.00)"
      result.getInterval(1) shouldBe "[0.00, 10.00]"
      result.getInterval(2) shouldBe "[10.00, Inf)"
    }

    "put all data in overflow bins when outside custom range" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(-100.0, -50.0, 200.0, 300.0).toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data).value.get

      result.bins(0).frequency shouldBe 2  // -100, -50 in left overflow
      result.bins(1).frequency shouldBe 0  // [0, 10) empty
      result.bins(2).frequency shouldBe 0  // [10, 20) empty
      result.bins(3).frequency shouldBe 2  // 200, 300 in right overflow
    }

    "route values exactly on first/last custom edge correctly with overflow" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(0.0, 10.0, 20.0).toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data).value.get

      // Edges after overflow: [-Inf, 0.0, 10.0, 20.0, Inf]
      // Bin 0: (-Inf, 0.0) - empty (0.0 excluded)
      // Bin 1: [0.0, 10.0) - contains 0.0
      // Bin 2: [10.0, 20.0] - contains 10.0, 20.0 (last interior bin inclusive)
      // Bin 3: [20.0, Inf] - empty (20.0 captured by last interior)
      result.bins(0).frequency shouldBe 0
      result.bins(1).frequency shouldBe 1  // 0.0
      result.bins(2).frequency shouldBe 2  // 10.0, 20.0
      result.bins(3).frequency shouldBe 0
    }

    "work with single custom edge pair and overflow" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(-1.0, 5.0, 15.0).toDF("values")
      val customEdges = Array(0.0, 10.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data).value.get

      result.numberOfBins shouldBe 3
      result.bins(0).frequency shouldBe 1  // -1.0 in left overflow
      result.bins(1).frequency shouldBe 1  // 5.0 in [0, 10]
      result.bins(2).frequency shouldBe 1  // 15.0 in right overflow
    }

    "work with Sum aggregation and overflow" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        (-5.0, 100), (5.0, 200), (15.0, 300), (25.0, 400)
      ).toDF("values", "amount")

      val customEdges = Array(0.0, 10.0, 20.0)
      val histogram = HistogramBinned(
        "values",
        customEdges = Some(customEdges),
        includeOverflowBins = true,
        aggregateFunction = Histogram.Sum("amount")
      )
      val result = histogram.calculate(data).value.get

      result.bins(0).frequency shouldBe 100  // left overflow sum
      result.bins(1).frequency shouldBe 200  // [0, 10) sum
      result.bins(2).frequency shouldBe 300  // [10, 20) sum
      result.bins(3).frequency shouldBe 400  // right overflow sum
    }

    "work with where filter and overflow" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        (1, -5.0), (2, 5.0), (3, 15.0), (4, 25.0)
      ).toDF("id", "values")

      val customEdges = Array(0.0, 10.0, 20.0)
      val histogram = HistogramBinned(
        "values",
        customEdges = Some(customEdges),
        includeOverflowBins = true,
        where = Some("id <= 2")
      )
      val result = histogram.calculate(data).value.get

      result.bins(0).frequency shouldBe 1  // -5.0 (id=1)
      result.bins(1).frequency shouldBe 1  // 5.0 (id=2)
      result.bins(2).frequency shouldBe 0  // filtered out
      result.bins(3).frequency shouldBe 0  // filtered out
    }

    "handle empty data with overflow" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq.empty[Double].toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get
      distribution.numberOfBins shouldBe 4
      distribution.bins.foreach(_.frequency shouldBe 0)
    }

    "handle all nulls with overflow" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(Option.empty[Double], None, None).toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data).value.get

      // 4 data/overflow bins all empty, nulls separate
      result.numberOfBins shouldBe 4
      result.bins(0).frequency shouldBe 0
      result.bins(1).frequency shouldBe 0
      result.bins(2).frequency shouldBe 0
      result.bins(3).frequency shouldBe 0
      result.nullCount shouldBe 3
    }

    "handle extreme values in overflow bins" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(Double.MinValue, -1e308, 5.0, 1e308, Double.MaxValue).toDF("values")
      val customEdges = Array(0.0, 10.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data).value.get

      result.bins(0).frequency shouldBe 2  // MinValue, -1e308 in left overflow
      result.bins(1).frequency shouldBe 1  // 5.0 in [0, 10]
      result.bins(2).frequency shouldBe 2  // 1e308, MaxValue in right overflow
    }

    "not add overflow bins when includeOverflowBins is false (default)" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(-5.0, 5.0, 25.0).toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data).value.get

      // Only 2 data bins, out-of-range values are dropped
      result.numberOfBins shouldBe 2
      result.bins(0).frequency shouldBe 1  // 5.0 in [0, 10)
      result.bins(1).frequency shouldBe 0  // [10, 20] empty (25.0 dropped)
      result.nullCount shouldBe 0  // no actual nulls
    }

    "drop out-of-range values separately from nulls when overflow disabled" in withSparkSession { spark =>
      import spark.implicits._

      // Mix of: in-range, out-of-range, and null values
      val data = Seq(Some(-10.0), Some(5.0), None, Some(15.0), Some(50.0), None).toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges))
      val result = histogram.calculate(data).value.get

      // Only data bins
      result.numberOfBins shouldBe 2
      result.bins(0).frequency shouldBe 1  // 5.0 in [0, 10)
      result.bins(1).frequency shouldBe 1  // 15.0 in [10, 20]

      // Only actual nulls in nullCount, not out-of-range
      result.nullCount shouldBe 2  // two None values

      // -10.0 and 50.0 are dropped (not counted anywhere)
      val totalCounted = result.bins.map(_.frequency).sum + result.nullCount
      totalCounted shouldBe 4  // 6 total rows - 2 dropped = 4
    }

    "throw when binCount < 3 with overflow enabled" in withSparkSession { spark =>
      import spark.implicits._
      import com.amazon.deequ.analyzers.runners.AnalysisRunner

      val data = Seq(1.0, 2.0, 3.0).toDF("values")
      val histogram = HistogramBinned("values", binCount = Some(2), includeOverflowBins = true)

      val analysis = AnalysisRunner.onData(data).addAnalyzer(histogram)
      val result = analysis.run()

      val metric = result.metricMap(histogram)
      metric.value.isFailure shouldBe true
    }

    "handle values exactly on overflow boundary edges" in withSparkSession { spark =>
      import spark.implicits._

      // Values exactly on the min and max (which become interior/overflow boundaries)
      val data = Seq(0.0, 5.0, 10.0, 15.0, 20.0).toDF("values")
      val customEdges = Array(0.0, 10.0, 20.0)

      val histogram = HistogramBinned("values", customEdges = Some(customEdges), includeOverflowBins = true)
      val result = histogram.calculate(data).value.get

      // Edges after overflow: [-Inf, 0.0, 10.0, 20.0, Inf]
      // Bin 0: (-Inf, 0.0) - empty (0.0 excluded from left overflow)
      // Bin 1: [0.0, 10.0) - contains 0.0, 5.0
      // Bin 2: [10.0, 20.0] - contains 10.0, 15.0, 20.0 (last interior inclusive)
      // Bin 3: (20.0, Inf] - empty
      result.bins(0).frequency shouldBe 0
      result.bins(1).frequency shouldBe 2  // 0.0, 5.0
      result.bins(2).frequency shouldBe 3  // 10.0, 15.0, 20.0
      result.bins(3).frequency shouldBe 0
    }
  }

  "HistogramBinned state" should {
    "compute metric from state without calling computeStateFrom (no NPE)" in withSparkSession { spark =>
      import spark.implicits._

      val edges = Array(0.0, 10.0, 20.0)
      val data = Seq("0" -> 3L, "1" -> 5L).toDF("values", "count")
      val frequencies = data.withColumnRenamed("count", Analyzers.COUNT_COL)

      val state = BinnedFrequencies(frequencies, 8L, edges)
      val analyzer = HistogramBinned("values", customEdges = Some(edges))
      val metric = analyzer.computeMetricFrom(Some(state))

      metric.value.isSuccess shouldBe true
      val dist = metric.value.get
      dist.bins(0).binStart shouldBe 0.0
      dist.bins(0).binEnd shouldBe 10.0
      dist.bins(0).frequency shouldBe 3
      dist.bins(1).binStart shouldBe 10.0
      dist.bins(1).binEnd shouldBe 20.0
      dist.bins(1).frequency shouldBe 5
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

    "throw IllegalArgumentException when custom edges has less than 2 values" in withSparkSession { spark =>
      val exception = intercept[IllegalArgumentException] {
        HistogramBinned("values", customEdges = Some(Array(1.0)))
      }
      exception.getMessage should include("Custom edges must have at least 2 values")
    }

    "throw IllegalAnalyzerParameterException when equal edges creates too many bins" in withSparkSession { spark =>
      import spark.implicits._
      import com.amazon.deequ.analyzers.runners.AnalysisRunner

      val data = Seq(1, 2, 3).toDF("values")
      val histogram = HistogramBinned("values", binCount = Some(HistogramBinned.MaximumAllowedDetailBins + 1))

      val analysis = AnalysisRunner.onData(data).addAnalyzer(histogram)
      val result = analysis.run()

      val metric = result.metricMap(histogram)
      metric.value.isFailure shouldBe true
      metric.value.asInstanceOf[Failure[Exception]].exception shouldBe a[IllegalAnalyzerParameterException]
      metric.value.asInstanceOf[Failure[Exception]].exception.getMessage should include(
        s"Cannot return histogram values for more than ${HistogramBinned.MaximumAllowedDetailBins} bins"
      )
    }

    "throw IllegalAnalyzerParameterException when custom edges creates too many bins" in withSparkSession { spark =>
      import spark.implicits._
      import com.amazon.deequ.analyzers.runners.AnalysisRunner

      val data = Seq(1, 2, 3).toDF("values")
      val tooManyEdges = Array.range(0, HistogramBinned.MaximumAllowedDetailBins + 2).map(_.toDouble)
      val histogram = HistogramBinned("values", customEdges = Some(tooManyEdges))

      val analysis = AnalysisRunner.onData(data).addAnalyzer(histogram)
      val result = analysis.run()

      val metric = result.metricMap(histogram)
      metric.value.isFailure shouldBe true
      metric.value.asInstanceOf[Failure[Exception]].exception shouldBe a[IllegalAnalyzerParameterException]
      metric.value.asInstanceOf[Failure[Exception]].exception.getMessage should include(
        s"Cannot return histogram values for more than ${HistogramBinned.MaximumAllowedDetailBins} bins"
      )
    }
  }
}
