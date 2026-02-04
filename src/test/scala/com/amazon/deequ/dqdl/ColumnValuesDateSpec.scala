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

class ColumnValuesDateSpec extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "ColumnValues with date expressions" should {

    val today = java.time.LocalDate.now()

    def createTestDataFrame(sparkSession: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
      import sparkSession.implicits._
      Seq(
        (1, "California", "CA", s"${today.minusDays(1).toString}"),
        (2, "New York", "NY", s"${today.minusDays(3).toString}"),
        (3, "New Jersey", "NJ", s"${today.minusDays(5).toString}"),
        (4, "Oregon", "OR", s"${today.minusDays(10).toString}")
      ).toDF("ID", "State Name", "State Abbreviation", "Order Date")
    }

    // Basic comparison operators with dynamic dates
    "pass for GREATER_THAN with now() - days when all dates are recent" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" > (now() - 15 days)]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail for GREATER_THAN with now() - days when some dates are old" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" > (now() - 4 days)]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    "pass for LESS_THAN with now() when all dates are in the past" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" < now()]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "pass for LESS_THAN_EQUAL_TO with now()" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" <= now()]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "pass for GREATER_THAN_EQUAL_TO with old date" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" >= (now() - 30 days)]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    // Static date comparisons
    "pass for GREATER_THAN with static date" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)
      val oldDate = today.minusDays(30).toString

      val ruleset = s"""Rules=[ColumnValues "Order Date" > "$oldDate"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail for LESS_THAN with static date when dates are newer" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)
      val oldDate = today.minusDays(30).toString

      val ruleset = s"""Rules=[ColumnValues "Order Date" < "$oldDate"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    // BETWEEN operator
    "pass for BETWEEN with date range covering all data" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" between (now() - 30 days) and now()]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail for BETWEEN with narrow date range" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" between (now() - 2 days) and now()]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    "pass for NOT BETWEEN excluding old dates" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" not between (now() - 100 days) and (now() - 50 days)]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    // IN operator
    "pass for IN with matching dates" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val specificDates = Seq(
        (1, "2024-01-01"),
        (2, "2024-12-25"),
        (3, "2024-01-01")
      ).toDF("ID", "Holiday")

      val ruleset = """Rules=[ColumnValues "Holiday" in ["2024-01-01", "2024-12-25"]]"""
      val results = EvaluateDataQuality.process(specificDates, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail for IN with non-matching dates" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val specificDates = Seq(
        (1, "2024-01-01"),
        (2, "2024-07-04"),
        (3, "2024-12-25")
      ).toDF("ID", "Holiday")

      val ruleset = """Rules=[ColumnValues "Holiday" in ["2024-01-01", "2024-12-25"]]"""
      val results = EvaluateDataQuality.process(specificDates, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    // NOT IN operator
    "pass for NOT IN excluding specific dates" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val specificDates = Seq(
        (1, "2024-02-14"),
        (2, "2024-03-17"),
        (3, "2024-04-01")
      ).toDF("ID", "Event Date")

      val ruleset = """Rules=[ColumnValues "Event Date" not in ["2024-01-01", "2024-12-25"]]"""
      val results = EvaluateDataQuality.process(specificDates, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    // Column name edge cases
    "work with column names containing spaces" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" < now()]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "work with column names containing dots (backticks)" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        (1, s"${today.minusDays(1).toString}"),
        (2, s"${today.minusDays(2).toString}")
      ).toDF("ID", "Some.Date")

      val ruleset = """Rules=[ColumnValues "`Some.Date`" < now()]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "work with nested columns" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val nestedDf = Seq(
        (1, (s"${today.minusDays(1).toString}", "NY")),
        (2, (s"${today.minusDays(2).toString}", "CA"))
      ).toDF("id", "state")

      val ruleset = """Rules=[ColumnValues "state._1" < now()]"""
      val results = EvaluateDataQuality.process(nestedDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    // Empty dataframe
    "pass for empty dataframes" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val emptyDF = Seq[(Int, String)]().toDF("ID", "Order Date")

      val ruleset = """Rules=[ColumnValues "Order Date" < now()]"""
      val results = EvaluateDataQuality.process(emptyDF, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    // NULL handling
    "handle NULL values in date column" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val dfWithNulls = Seq(
        (1, Some(s"${today.minusDays(1).toString}")),
        (2, None),
        (3, Some(s"${today.minusDays(2).toString}"))
      ).toDF("ID", "Order Date")

      val ruleset = """Rules=[ColumnValues "Order Date" < now()]"""
      val results = EvaluateDataQuality.process(dfWithNulls, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }
  }

  "ColumnValues date with where clause" should {

    val today = java.time.LocalDate.now()

    def createTestDataFrame(sparkSession: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
      import sparkSession.implicits._
      Seq(
        (1, "California", "CA", s"${today.minusDays(1).toString}"),
        (2, "New York", "NY", s"${today.minusDays(3).toString}"),
        (3, "New Jersey", "NJ", s"${today.minusDays(5).toString}"),
        (4, "Oregon", "OR", s"${today.minusDays(10).toString}")
      ).toDF("ID", "State Name", "State Abbreviation", "Order Date")
    }

    "pass when filtered rows satisfy condition" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" > (now() - 4 days) where "ID <= 2"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail when filtered rows don't satisfy condition" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" > (now() - 2 days) where "ID <= 2"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    "pass when all rows are filtered out" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" > now() where "ID > 100"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[String]("FailureReason") should include("No rows matched the filter")
    }

    "fail for invalid where clause" in withSparkSession { sparkSession =>
      val df = createTestDataFrame(sparkSession)

      val ruleset = """Rules=[ColumnValues "Order Date" < now() where "%%invalid%%"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("where clause is invalid")
    }
  }

  "ColumnValues date error handling" should {

    "fail when column does not exist" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq((1, "2024-01-01")).toDF("ID", "Date")

      val ruleset = """Rules=[ColumnValues "NonExistentColumn" < now()]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("does not exist")
    }
  }
}
