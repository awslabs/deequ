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
}
