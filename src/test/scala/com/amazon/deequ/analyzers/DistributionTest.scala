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
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DistributionTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Histogram" should {
    "create categorical keys sorted by frequency for string data" in withSparkSession { spark =>
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
    }

    "create categorical keys sorted by frequency for boolean data" in withSparkSession { spark =>
      import spark.implicits._

      val data = Seq(true, true, true, false, false, true).toDF("Binary")

      val histogram = Histogram("Binary")
      val result = histogram.calculate(data)

      result.value.isSuccess shouldBe true
      val distribution = result.value.get

      // Should be sorted by descending frequency
      val keys = distribution.values.keys.toSeq
      keys.head shouldBe "true"
      keys.last shouldBe "false"
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
      val histogram = Histogram("values", maxDetailBins = 5)
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
    }
  }
}
