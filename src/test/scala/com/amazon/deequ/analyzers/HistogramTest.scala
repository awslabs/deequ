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
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.functions.udf
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Failure

class HistogramTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Histogram (categorical)" should {
    "create categorical keys sorted by frequency for string data with ratio" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        "Blue", "Blue", "Blue", "Blue",
        "Green", "Green", "Green",
        "Red",
        "Yellow", "Yellow", "Yellow", "Yellow", "Yellow"
      ).toDF("colors")

      val histogram = Histogram("colors")
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Should be sorted by descending frequency (Yellow=5, Blue=4, Green=3, Red=1)
      val keys = distribution.values.keys.toSeq
      keys.head shouldBe "Yellow"
      keys.last shouldBe "Red"

      // Verify frequencies
      distribution.values("Yellow").absolute shouldBe 5
      distribution.values("Blue").absolute shouldBe 4
      distribution.values("Green").absolute shouldBe 3
      distribution.values("Red").absolute shouldBe 1

      // Verify ratios
      distribution.values("Yellow").ratio shouldBe 5.0 / 13.0
      distribution.values("Blue").ratio shouldBe 4.0 / 13.0
      distribution.values("Green").ratio shouldBe 3.0 / 13.0
      distribution.values("Red").ratio shouldBe 1.0 / 13.0
    }

    "create categorical keys sorted by frequency for string data" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        "Very Satisfied", "Very Satisfied", "Very Satisfied", "Very Satisfied", "Very Satisfied", "Very Satisfied",
        "Satisfied", "Satisfied", "Satisfied", "Satisfied",
        "Neutral", "Neutral", "Neutral",
        "Dissatisfied", "Dissatisfied",
        "Very Dissatisfied"
      ).toDF("satisfaction")

      val histogram = Histogram(
        "satisfaction",
        computeFrequenciesAsRatio = false // disable ratio computing
      )
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Should be sorted by descending frequency
      // (Very Satisfied=6, Satisfied=4, Neutral=3, Dissatisfied=2, Very Dissatisfied=1)
      val keys = distribution.values.keys.toSeq
      keys.head shouldBe "Very Satisfied"
      keys.last shouldBe "Very Dissatisfied"

      // Verify frequencies
      distribution.values("Very Satisfied").absolute shouldBe 6
      distribution.values("Satisfied").absolute shouldBe 4
      distribution.values("Neutral").absolute shouldBe 3
      distribution.values("Dissatisfied").absolute shouldBe 2
      distribution.values("Very Dissatisfied").absolute shouldBe 1

      // Verify ratios equal absolute values (no ratio computation)
      distribution.values("Very Satisfied").ratio shouldBe 6.0
      distribution.values("Satisfied").ratio shouldBe 4.0
      distribution.values("Neutral").ratio shouldBe 3.0
      distribution.values("Dissatisfied").ratio shouldBe 2.0
      distribution.values("Very Dissatisfied").ratio shouldBe 1.0
    }

    "create categorical keys sorted by frequency for boolean data" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(true, true, true, false, false, true).toDF("Binary")

      val histogram = Histogram("Binary", computeFrequenciesAsRatio = false)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Should be sorted by descending frequency
      val keys = distribution.values.keys.toSeq
      keys.head shouldBe "true"
      keys.last shouldBe "false"

      // Verify frequencies
      distribution.values("true").absolute shouldBe 4
      distribution.values("false").absolute shouldBe 2
    }

    "limit detailed results to maxDetailBins parameter" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        "A", "A", "A", "A", "A", "A", "A", "A", "A", "A",
        "B", "B", "B", "B", "B", "B", "B", "B", "B",
        "C", "C", "C", "C", "C", "C", "C", "C",
        "D", "D", "D", "D", "D", "D", "D",
        "E", "E", "E", "E", "E", "E",
        "F", "F", "F", "F", "F",
        "G", "G", "G", "G",
        "H", "H", "H",
        "I", "I",
        "J"
      ).toDF("values")

      // truncate at 5 bins
      val histogram = Histogram("values", maxDetailBins = 5, computeFrequenciesAsRatio = false)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Should have 10 total unique values
      distribution.numberOfBins shouldBe 10

      // But only top 5 most frequent should be in detailed results
      distribution.values.size shouldBe 5

      // Should be sorted by descending frequency
      val keys = distribution.values.keys.toSeq
      keys.head shouldBe "A"
      keys.last shouldBe "E"

      // Should contain the top 5 most frequent values (A, B, C, D, E)
      distribution.values should contain key ("A")
      distribution.values should contain key ("B")
      distribution.values should contain key ("C")
      distribution.values should contain key ("D")
      distribution.values should contain key ("E")

      // Should NOT contain the less frequent values (F, G, H, I, J)
      distribution.values should not contain key ("F")
      distribution.values should not contain key ("G")
      distribution.values should not contain key ("H")
      distribution.values should not contain key ("I")
      distribution.values should not contain key ("J")

      // Verify frequencies
      distribution.values("A").absolute shouldBe 10
      distribution.values("B").absolute shouldBe 9
      distribution.values("C").absolute shouldBe 8
      distribution.values("D").absolute shouldBe 7
      distribution.values("E").absolute shouldBe 6
    }

    "group values using binningUdf for categorical data" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq("excellent", "very good", "good", "okay", "poor", "terrible", "excellent", "good")
        .toDF("satisfaction")

      val groupingUdf = udf((rating: String) => rating match {
        case "excellent" | "very good" => "positive"
        case "good" | "okay" => "neutral"
        case "poor" | "terrible" => "negative"
      })

      val histogram = Histogram("satisfaction", Some(groupingUdf))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 3
      distribution.values.size shouldBe 3

      // Verify grouped categories
      distribution.values should contain key ("positive")
      distribution.values should contain key ("neutral")
      distribution.values should contain key ("negative")

      distribution.values("positive").absolute shouldBe 3 // excellent, very good, excellent
      distribution.values("neutral").absolute shouldBe 3 // good, okay, good
      distribution.values("negative").absolute shouldBe 2 // poor, terrible
    }

    "group numerical values using binningUdf for age ranges" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(15, 25, 35, 45, 70, 80, 16, 30).toDF("age")

      val ageGroupUdf = udf((age: Int) => age match {
        case x if x < 18 => "minor"
        case x if x < 65 => "adult"
        case _ => "senior"
      })

      val histogram = Histogram("age", Some(ageGroupUdf))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.numberOfBins shouldBe 3
      distribution.values.size shouldBe 3

      // Verify age groups
      distribution.values should contain key ("minor")
      distribution.values should contain key ("adult")
      distribution.values should contain key ("senior")

      distribution.values("minor").absolute shouldBe 2 // 15, 16
      distribution.values("adult").absolute shouldBe 4 // 25, 35, 45, 30
      distribution.values("senior").absolute shouldBe 2 // 70, 80
    }

    "aggregate sum works as expected" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        ("Electronics", 100),
        ("Electronics", 200),
        ("Furniture", 150),
        ("Electronics", 300),
        ("Furniture", 250),
        ("Cosmetics", 75)
      ).toDF("category", "sales")
      val histogram = Histogram(
        "category", aggregateFunction = Histogram.Sum("sales"), computeFrequenciesAsRatio = false
      )
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Should have 3 categories
      distribution.numberOfBins shouldBe 3
      distribution.values.size shouldBe 3

      // Verify sum aggregation (not counts, but sums of sales)
      distribution.values("Electronics").absolute shouldBe 600 // 100 + 200 + 300
      distribution.values("Furniture").absolute shouldBe 400   // 150 + 250
      distribution.values("Cosmetics").absolute shouldBe 75    // 75

      // Should be sorted by descending sum (Electronics=600, Furniture=400, Cosmetics=75)
      val keys = distribution.values.keys.toSeq
      keys.head shouldBe "Electronics"
      keys.last shouldBe "Cosmetics"
    }

    "aggregate sum works as expected with nulls" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        (Some("Electronics"), Some(100)),
        (None, Some(999)),                // null category -> becomes "NullValue"
        (Some("Electronics"), Some(200)),
        (Some("Furniture"), None),        // null sales -> treated as 0 in sum
        (Some("Electronics"), Some(300)),
        (Some("Furniture"), Some(250)),
        (Some("Cosmetics"), Some(75)),
        (None, Some(888))                 // null category -> becomes "NullValue"
      ).toDF("category", "sales")

      val histogram = Histogram("category", aggregateFunction = Histogram.Sum("sales"))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Should have 4 categories (including NullValue for null categories)
      distribution.numberOfBins shouldBe 4
      distribution.values.size shouldBe 4

      // Verify sum aggregation with nulls handled
      distribution.values("Electronics").absolute shouldBe 600  // 100 + 200 + 300
      distribution.values("Furniture").absolute shouldBe 250    // 0 + 250 (null sales treated as 0)
      distribution.values("Cosmetics").absolute shouldBe 75     // 75
      distribution.values("NullValue").absolute shouldBe 1887   // 999 + 888 (null categories)

      // Should be sorted by descending sum
      val keys = distribution.values.keys.toSeq
      keys.head shouldBe "NullValue"  // highest sum: 1887
      keys(1) shouldBe "Electronics"  // second highest: 600
    }

    "sort tied frequencies alphabetically" in withSparkSession { spark =>
      import spark.implicits._

      val data = (Seq.fill(51)("Iris-setosa") ++
        Seq.fill(50)("Iris-virginica") ++
        Seq.fill(50)("Iris-versicolor") ++
        Seq.fill(10)("Iris-xiphium")).toDF("class")

      val histogram = Histogram("class", computeFrequenciesAsRatio = false)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val keys = result.value.get.values.keys.toSeq

      keys shouldBe Seq("Iris-setosa", "Iris-versicolor", "Iris-virginica", "Iris-xiphium")
    }

    "handle all null data gracefully" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(None: Option[Double], None, None, None).toDF("values")

      val histogram = Histogram("values", maxDetailBins = 5, computeFrequenciesAsRatio = false)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Should only have 1 bin of NullValue
      distribution.numberOfBins shouldBe 1
      distribution.values.size shouldBe 1

      distribution.values("NullValue").absolute shouldBe 4
    }
  }

  "Histogram (tail)" should {
    "compute tailCount when categories exceed maxDetailBins" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        "A", "A", "A", "A", "A",  // 5
        "B", "B", "B", "B",       // 4
        "C", "C", "C",            // 3
        "D", "D",                 // 2
        "E"                       // 1
      ).toDF("category")

      val histogram = Histogram("category", maxDetailBins = 3)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Top 3 by frequency: A=5, B=4, C=3
      distribution.values.size shouldBe 3
      distribution.values("A").absolute shouldBe 5
      distribution.values("B").absolute shouldBe 4
      distribution.values("C").absolute shouldBe 3

      // Tail: D=2 + E=1 = 3
      distribution.tailCount shouldBe 3

      // Total categories
      distribution.numberOfBins shouldBe 5
    }

    "have zero tailCount when categories fit within maxDetailBins" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq("A", "A", "B", "B", "C").toDF("category")

      val histogram = Histogram("category", maxDetailBins = 10)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.values.size shouldBe 3
      distribution.tailCount shouldBe 0
    }

    "have zero tailCount when categories exactly equal maxDetailBins" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq("A", "A", "B", "C").toDF("category")

      val histogram = Histogram("category", maxDetailBins = 3)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.values.size shouldBe 3
      distribution.tailCount shouldBe 0
    }

    "compute tailCount with maxDetailBins = 1" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq("A", "A", "A", "B", "B", "C").toDF("category")

      val histogram = Histogram("category", maxDetailBins = 1)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.values.size shouldBe 1
      distribution.values("A").absolute shouldBe 3
      // Tail: B=2 + C=1 = 3
      distribution.tailCount shouldBe 3
    }

    "compute tailCount with nulls present" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(Some("A"), Some("A"), Some("B"), None, Some("C"), None).toDF("category")

      val histogram = Histogram("category", maxDetailBins = 2)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Top 2: A=2, NullValue=2 (sorted by freq desc, then name asc)
      distribution.values.size shouldBe 2
      // Tail: B=1 + C=1 = 2
      distribution.tailCount shouldBe 2
    }

    "compute tailCount with Sum aggregation" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        ("A", 100), ("A", 200),
        ("B", 50), ("B", 75),
        ("C", 10), ("D", 5)
      ).toDF("category", "amount")

      val histogram = Histogram("category", maxDetailBins = 2,
        aggregateFunction = Histogram.Sum("amount"))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Top 2 by sum: A=300, B=125
      distribution.values.size shouldBe 2
      distribution.values("A").absolute shouldBe 300
      distribution.values("B").absolute shouldBe 125
      // Tail: C=10 + D=5 = 15
      distribution.tailCount shouldBe 15
    }

    "compute tailCount larger than any individual top bin" in withSparkSession { spark =>
      import spark.implicits._

      // 50 rare categories with 2 each = 100 in tail
      // Top 3 categories have 10, 8, 6
      val topData = Seq.fill(10)("Top1") ++ Seq.fill(8)("Top2") ++ Seq.fill(6)("Top3")
      val tailData = (1 to 50).flatMap(i => Seq.fill(2)(s"Rare$i"))
      val data = (topData ++ tailData).toDF("category")

      val histogram = Histogram("category", maxDetailBins = 3)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      distribution.values.size shouldBe 3
      distribution.values("Top1").absolute shouldBe 10
      // Tail = 100, larger than any single top bin
      distribution.tailCount shouldBe 100
      distribution.tailCount should be > distribution.values("Top1").absolute
    }

    "break ties alphabetically when frequencies are equal" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        "Banana", "Banana",
        "Apple", "Apple",
        "Cherry", "Cherry",
        "Date", "Date"
      ).toDF("category")

      val histogram = Histogram("category", maxDetailBins = 2)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // All have frequency 2, tie-broken alphabetically ascending
      distribution.values.size shouldBe 2
      val keys = distribution.values.keys.toSeq
      keys should contain("Apple")
      keys should contain("Banana")
      // Cherry and Date go to tail
      distribution.tailCount shouldBe 4
    }

    "have correct ratios relative to total including tail" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        "A", "A", "A", "A", "A",  // 5
        "B", "B", "B",            // 3
        "C", "C"                  // 2
      ).toDF("category")

      val histogram = Histogram("category", maxDetailBins = 2)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Ratios should be relative to total (10), not just top N
      distribution.values("A").ratio shouldBe 0.5 +- 0.001
      distribution.values("B").ratio shouldBe 0.3 +- 0.001
      distribution.tailCount shouldBe 2
    }

    "throw when maxDetailBins is 0" in withSparkSession { spark =>
      import spark.implicits._
      import com.amazon.deequ.analyzers.runners.AnalysisRunner

      val data = Seq("A", "B").toDF("category")
      val histogram = Histogram("category", maxDetailBins = 0)

      val analysis = AnalysisRunner.onData(data).addAnalyzer(histogram)
      val result = analysis.run()

      val metric = result.metricMap(histogram)
      metric.value.isFailure shouldBe true
    }

    "compute tailCount with where filter" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(
        (1, "A"), (2, "A"), (3, "A"),
        (4, "B"), (5, "B"),
        (6, "C"), (7, "C"),
        (8, "D")
      ).toDF("id", "category")

      val histogram = Histogram("category", maxDetailBins = 2, where = Some("id <= 6"))
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Filtered data: A=3, B=2, C=1 (id 6 has C, id 7 excluded)
      distribution.values.size shouldBe 2
      distribution.values("A").absolute shouldBe 3
      distribution.values("B").absolute shouldBe 2
      distribution.tailCount shouldBe 1  // C=1
    }

    "compute tailCount with binningUdf" in withSparkSession { spark =>
      import spark.implicits._
      import org.apache.spark.sql.functions.udf

      val data = Seq("US", "USA", "UK", "GB", "France", "Germany", "Italy").toDF("country")

      // UDF groups US/USA and UK/GB together
      val normalize = udf((s: String) => s match {
        case "US" | "USA" => "US"
        case "UK" | "GB" => "UK"
        case other => other
      })

      val histogram = Histogram("country", binningUdf = Some(normalize), maxDetailBins = 2)
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // After UDF: US=2, UK=2, France=1, Germany=1, Italy=1
      distribution.values.size shouldBe 2
      // Tail: remaining categories after UDF grouping
      distribution.tailCount shouldBe 3  // France + Germany + Italy
    }

    "emit tailCount in flatten() when tail exists" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq("A", "A", "A", "B", "B", "C").toDF("category")

      val histogram = Histogram("category", maxDetailBins = 1)
      val metric = histogram.calculate(data)

      val flattened = metric.flatten()
      flattened.exists(m => m.name == "Histogram.tailCount" && m.value.get == 3.0) shouldBe true
    }

    "not emit tailCount in flatten() when no tail" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq("A", "B").toDF("category")

      val histogram = Histogram("category", maxDetailBins = 10)
      val metric = histogram.calculate(data)

      val flattened = metric.flatten()
      flattened.exists(_.name == "Histogram.tailCount") shouldBe false
    }
  }
}
