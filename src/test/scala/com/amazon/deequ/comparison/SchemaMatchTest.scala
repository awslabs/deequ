/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
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

class SchemaMatchTest extends AnyWordSpec with SparkContextSpec {
  "SchemaMatch" should {
    "succeed when schema are equal" in withSparkSession { spark =>
      import spark.implicits._
      val primaryDf = Seq(
        (1, "California", "CA"),
        (2, "New York", "NY")
      ).toDF("id", "State Name", "State Abbreviation")

      val referenceDf = Seq(
        (3, "New Jersey", "NJ"),
        (4, "Texas", "TX")
      ).toDF("id", "State Name", "State Abbreviation")

      val result = SchemaMatch.matchSchema(primaryDf, referenceDf, _ > 0.9)
      assert(result.isInstanceOf[ComparisonSucceeded])
      assert(result.asInstanceOf[ComparisonSucceeded].ratio == 1.0)
    }

    "succeed when some columns do not match" in withSparkSession { spark =>
      import spark.implicits._
      val primaryDf = Seq(
        (1, "California", "CA"),
        (2, "New York", "NY")
      ).toDF("id", "State Name", "State Abbreviation")

      val referenceDf = Seq(
        (3, "New Jersey", "NJ"),
        (4, "Texas", "TX")
      ).toDF("id", "State Name", "Abbreviation")

      val result = SchemaMatch.matchSchema(primaryDf, referenceDf, _ > 0.65)
      assert(result.isInstanceOf[ComparisonSucceeded])
      assert(result.asInstanceOf[ComparisonSucceeded].ratio == 2.0 / 3.0)
    }

    "fails when schema does not match" in withSparkSession { spark =>
      import spark.implicits._
      val primaryDf = Seq(
        (1, "California", "CA"),
        (2, "New York", "NY")
      ).toDF("id", "State Name", "State Abbreviation")
      val referenceDf = Seq(
        (3, "New Jersey", "NJ", "Garden State"),
        (4, "Texas", "TX", "Lone Star State")
      ).toDF("Number", "Full State Name", "Abbreviation", "Nickname")

      val result = SchemaMatch.matchSchema(primaryDf, referenceDf, _ == 1.0)
      assert(result.isInstanceOf[ComparisonFailed])
    }

    "succeed when column order differs but names and types match" in withSparkSession { spark =>
      import spark.implicits._
      val primaryDf = Seq(
        (1, "CA"),
        (2, "NY")
      ).toDF("id", "State Abbreviation")

      val referenceDf = Seq(
        ("CA", 3),
        ("TX", 4)
      ).toDF("State Abbreviation", "id")

      val result = SchemaMatch.matchSchema(primaryDf, referenceDf, _ == 1.0)
      assert(result.isInstanceOf[ComparisonSucceeded])
      assert(result.asInstanceOf[ComparisonSucceeded].ratio == 1.0)
    }

    "fail when dataframes have no rows but matching schemas" in withSparkSession { spark =>
      import spark.implicits._
      val primaryDf = Seq(
        (1, "CA"),
        (2, "NY")
      ).toDF("id", "State Abbreviation")
      val referenceDf = spark.emptyDataFrame

      val result = SchemaMatch.matchSchema(primaryDf, referenceDf, _ == 1.0)
      assert(result.isInstanceOf[ComparisonFailed])
    }

    "fail when type mismatch with same column name" in withSparkSession { spark =>
      import spark.implicits._
      val primaryDf = Seq((1, "test")).toDF("id", "name")
      val referenceDf = Seq(("1", "test")).toDF("id", "name")

      val result = SchemaMatch.matchSchema(primaryDf, referenceDf, _ == 1.0)
      assert(result.isInstanceOf[ComparisonFailed])
    }
  }
}
