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

package com.amazon.deequ.dqdl

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DataFreshnessSpec extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "DataFreshness" should {

    "work for dataframes with nested columns" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val today = java.time.LocalDate.now()
      val nestedDf = Seq(
        (1, (s"${today.minusDays(1).toString}", "NY")),
        (2, (s"${today.minusDays(2).toString}", "WI")),
        (3, (s"${today.minusDays(3).toString}", "TX")),
        (4, (s"${today.minusDays(4).toString}", "CA"))
      ).toDF("id", "state")

      val ruleset = """Rules=[DataFreshness "state._1" < 200 hours]"""
      val results = EvaluateDataQuality.process(nestedDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "work for dataframes with columns with . in name" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val today = java.time.LocalDate.now()
      val stateDf = Seq(
        ("California", "CA", s"${today.minusDays(1).toString}"),
        ("New York", "NY", s"${today.minusDays(2).toString}"),
        ("New Jersey", "NJ", s"${today.minusDays(3).toString}"),
        ("Oregon", "OR", s"${today.minusDays(4).toString}")
      ).toDF("State Name", "State Abbreviation", "Some.Date")

      val ruleset = """Rules=[DataFreshness "`Some.Date`" < 200 hours]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "work for empty dataframes" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val emptyDF = Seq[(String, String, String)]().toDF("State Name", "State Abbreviation", "Some Date")

      val ruleset = """Rules=[DataFreshness "Some Date" < 200 hours]"""
      val results = EvaluateDataQuality.process(emptyDF, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }
  }

  "DataFreshness Conditions" should {

    def createTestDataFrame(sparkSession: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
      val now = java.time.LocalDateTime.now()
      import sparkSession.implicits._
      Seq(
        (1, "California", "CA", s"${now.minusDays(1).toString}"),
        (2, "New York", "NY", s"${now.minusDays(2).toString}"),
        (3, "New Jersey", "NJ", s"${now.minusDays(3).toString}"),
        (4, "Oregon", "OR", s"${now.minusDays(5).toString}")
      ).toDF("ID", "State Name", "State Abbreviation", "Some Date")
    }

    "work for - between - operator" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" between 12 hours and 60 hours]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // Rows 1 and 2 should pass (24h and 48h are between 12 and 60)
    }

    "work for - not between - operator" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" not between 50 hours and 90 hours]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // Rows 1, 2, and 4 should pass (not between 50-90)
    }

    "work for - greater than / equal to - operator" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" >= 50 hours]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // Rows 3 and 4 should pass (72h and 120h >= 50)
    }

    "work for - greater than - operator" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" > 100 hours]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // Only row 4 should pass (120h > 100)
    }

    "work for - greater than - operator - minutes" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" > 4319 minutes]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // 4319 minutes = 71.983 hours, rows 3 and 4 should pass
    }

    "work for - less than / equal to - operator" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" <= 30 hours]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // Only row 1 should pass (24h <= 30)
    }

    "work for - less than - operator" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" < 20 hours]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // No rows should pass (all > 20h)
    }

    "work for - equals - operator" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" = 24 hours]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // Only row 1 should pass (exactly 24h)
    }

    "work for - not equals - operator" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" != 72 hours]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // Rows 1, 2, and 4 should pass (not equal to 72h)
    }

    "work for - not in - operator" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" not in [24 hours, 48 hours]]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      // Rows 3 and 4 should pass (not 24h or 48h)
    }
  }

  "DataFreshness with where clause" should {

    val today = java.time.LocalDate.now()

    def createTestDataFrame(sparkSession: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
      import sparkSession.implicits._
      Seq(
        (1, "California", "CA", s"${today.minusDays(1).toString}"),
        (2, "New York", "NY", s"${today.minusDays(2).toString}"),
        (3, "New Jersey", "NJ", s"${today.minusDays(3).toString}"),
        (4, "Oregon", "OR", s"${today.minusDays(5).toString}")
      ).toDF("ID", "State Name", "State Abbreviation", "Some Date")
    }

    "work for rule with where clause" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" < 100 hours where "ID > 2"]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      // Should have metric with where clause hash
      metrics.values.head should be(0.5) // 1 out of 2 filtered rows pass
    }

    "work for rule where all rows are filtered out" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" < 100 hours where "ID > 4"]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[String]("FailureReason") should include("No rows matched the filter")
    }

    "fail for rule with an invalid where clause" in withSparkSession { sparkSession =>
      val stateDf = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[DataFreshness "Some Date" < 100 hours where "%%'' > %%%%"]"""
      val results = EvaluateDataQuality.process(stateDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("where clause is invalid")
    }
  }
}
