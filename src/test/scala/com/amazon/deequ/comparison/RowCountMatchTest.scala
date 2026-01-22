/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.comparison

import com.amazon.deequ.SparkContextSpec
import org.scalatest.wordspec.AnyWordSpec

class RowCountMatchTest extends AnyWordSpec with SparkContextSpec {
  "RowCountMatch" should {
    "return ratio of 1.0 when row counts are equal" in withSparkSession { spark =>
      import spark.implicits._

      val primaryDF = Seq(
        ("California", "CA"),
        ("New York", "NY"),
        ("Oregon", "OR")
      ).toDF("State Name", "State Abbreviation")

      val referenceDF = Seq(
        ("Texas", "TX"),
        ("Washington", "WA"),
        ("Montana", "MT")
      ).toDF("State Name", "State Abbreviation")

      val result = RowCountMatch.matchRowCounts(primaryDF, referenceDF, _ == 1.0)
      assert(result.isInstanceOf[ComparisonSucceeded])
      assert(result.asInstanceOf[ComparisonSucceeded].ratio == 1.0)
    }

    "check rule with primary count lower than reference" in withSparkSession { spark =>
      import spark.implicits._

      val primaryDF = Seq(
        ("California", "CA"),
        ("New York", "NY"),
        ("New Jersey", "NJ"),
        ("Oregon", "OR")
      ).toDF("State Name", "State Abbreviation")

      val referenceDF = Seq(
        ("California", "CA"),
        ("New York", "NY"),
        ("New Jersey", "NJ"),
        ("Oregon", "OR"),
        ("Texas", "TX"),
        ("Washington", "WA"),
        ("Montana", "MT")
      ).toDF("State Name", "State Abbreviation")

      val expectedRatio = primaryDF.count().toDouble / referenceDF.count()

      // = 1.0 should fail (ratio is ~0.57)
      val result1 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, _ == 1.0)
      assert(result1.isInstanceOf[ComparisonFailed])

      // != 1.0 should pass
      val result2 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, _ != 1.0)
      assert(result2.isInstanceOf[ComparisonSucceeded])

      // >= 0.57 should pass
      val result3 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, _ >= 0.57)
      assert(result3.isInstanceOf[ComparisonSucceeded])

      // between 0.8 and 0.9 should fail
      val result4 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, r => r >= 0.8 && r <= 0.9)
      assert(result4.isInstanceOf[ComparisonFailed])

      // not between 0.8 and 0.9 should pass
      val result5 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, r => !(r >= 0.8 && r <= 0.9))
      assert(result5.isInstanceOf[ComparisonSucceeded])

      // not between 0.5 and 0.58 should fail (ratio ~0.57 is in range)
      val result6 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, r => !(r >= 0.5 && r <= 0.58))
      assert(result6.isInstanceOf[ComparisonFailed])

      // < 0.58 should pass
      val result7 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, _ < 0.58)
      assert(result7.isInstanceOf[ComparisonSucceeded])
      assert(result7.asInstanceOf[ComparisonSucceeded].ratio == expectedRatio)
    }

    "check rule with primary count higher than reference" in withSparkSession { spark =>
      import spark.implicits._

      val primaryDF = Seq(
        ("California", "CA"),
        ("New York", "NY"),
        ("New Jersey", "NJ"),
        ("Oregon", "OR"),
        ("Texas", "TX"),
        ("Washington", "WA")
      ).toDF("State Name", "State Abbreviation")

      val referenceDF = Seq(
        ("California", "CA"),
        ("New York", "NY"),
        ("New Jersey", "NJ"),
        ("Oregon", "OR")
      ).toDF("State Name", "State Abbreviation")

      val expectedRatio = primaryDF.count().toDouble / referenceDF.count() // 1.5

      // >= 1.0 should pass
      val result1 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, _ >= 1.0)
      assert(result1.isInstanceOf[ComparisonSucceeded])

      // < 1.0 should fail
      val result2 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, _ < 1.0)
      assert(result2.isInstanceOf[ComparisonFailed])

      // between 0.9 and 1.2 should fail (ratio is 1.5)
      val result3 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, r => r >= 0.9 && r <= 1.2)
      assert(result3.isInstanceOf[ComparisonFailed])

      // between 0.9 and 1.55 should pass
      val result4 = RowCountMatch.matchRowCounts(primaryDF, referenceDF, r => r >= 0.9 && r <= 1.55)
      assert(result4.isInstanceOf[ComparisonSucceeded])
      assert(result4.asInstanceOf[ComparisonSucceeded].ratio == expectedRatio)
    }

    // Edge case tests - matches AWS Glue Data Quality behavior
    // See: https://docs.aws.amazon.com/glue/latest/dg/dqdl-rule-types-RowCountMatch.html

    "return Infinity when reference dataset is empty" in withSparkSession { spark =>
      import spark.implicits._

      val primaryDF = Seq(("California", "CA")).toDF("State Name", "State Abbreviation")
      val referenceDF = spark.emptyDataFrame

      val result = RowCountMatch.matchRowCounts(primaryDF, referenceDF, _ => true)
      assert(result.isInstanceOf[ComparisonSucceeded])
      assert(result.asInstanceOf[ComparisonSucceeded].ratio.isInfinity)
    }

    "return NaN when both datasets are empty" in withSparkSession { spark =>
      val primaryDF = spark.emptyDataFrame
      val referenceDF = spark.emptyDataFrame

      // NaN comparisons always return false, so assertion fails
      val result = RowCountMatch.matchRowCounts(primaryDF, referenceDF, _ >= 0.9)
      assert(result.isInstanceOf[ComparisonFailed])
      assert(result.asInstanceOf[ComparisonFailed].ratio.isNaN)
    }
  }
}
