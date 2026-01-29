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

class ColumnDataTypeSpec extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "ColumnDataType" should {

    "work for dataframes with nested columns" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val nestedDf = Seq(
        (1, ("2023-01-01", "NY")),
        (2, ("2023-02-15", "WI")),
        (3, ("2023-03-20", "TX"))
      ).toDF("id", "info")

      val ruleset = """Rules=[ColumnDataType "info._1" = "DATE"]"""
      val results = EvaluateDataQuality.process(nestedDf, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "work for dataframes with columns with . in name" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "2023-02-15"),
        (3, "2023-03-20")
      ).toDF("id", "Some.Date")

      val ruleset = """Rules=[ColumnDataType "`Some.Date`" = "DATE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "work for empty dataframes" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val emptyDF = Seq[(Int, String)]().toDF("id", "date_col")

      val ruleset = """Rules=[ColumnDataType "date_col" = "DATE"]"""
      val results = EvaluateDataQuality.process(emptyDF, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }
  }

  "ColumnDataType for DATE type" should {

    "pass when all values can be cast to DATE" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "2023-02-15"),
        (3, "2023-03-20")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      metrics.values.head should be(1.0)
    }

    "fail when some values cannot be cast to DATE" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "not-a-date"),
        (3, "2023-03-20")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    "fail when all values cannot be cast to DATE" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "not-a-date"),
        (2, "also-not-a-date"),
        (3, "still-not-a-date")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      metrics.values.head should be(0.0)
    }

    "pass with format tag for ISO date format yyyy-MM-dd" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "2023-02-15"),
        (3, "2023-03-20")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "yyyy-MM-dd"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "pass with format tag for non-ISO date format dd-MM-yyyy" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "01-01-2023"),
        (2, "15-02-2023"),
        (3, "20-03-2023")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "dd-MM-yyyy"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "pass with format tag for MM-dd-yyyy" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "01-01-2023"),
        (2, "02-15-2023"),
        (3, "03-20-2023")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "MM-dd-yyyy"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail without format tag for non-ISO date format" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "01-01-2023"),
        (2, "15-02-2023"),
        (3, "20-03-2023")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    "pass with partial date format MM/yyyy" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "01/2023"),
        (2, "02/2023"),
        (3, "03/2023")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "MM/yyyy"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "pass with partial date format yyyy-MM" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01"),
        (2, "2023-02"),
        (3, "2023-03")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "yyyy-MM"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "pass with partial date format MM-yyyy" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "01-2023"),
        (2, "02-2023"),
        (3, "03-2023")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "MM-yyyy"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "pass with partial date format dd-MM" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "01-01"),
        (2, "15-02"),
        (3, "20-03")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "dd-MM"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail with invalid/unsupported format tag" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "2023-02-15")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "invalid-format"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    "fail with wrong format for data" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "2023-02-15")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "dd-MM-yyyy"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    "pass with slash-based format dd/MM/yyyy" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "01/01/2023"),
        (2, "15/02/2023"),
        (3, "20/03/2023")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" with format = "dd/MM/yyyy"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }
  }

  "ColumnDataType for numeric types" should {

    "pass for DOUBLE when all values are valid" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "11.5"),
        (2, "12.0"),
        (3, "13.45")
      ).toDF("id", "num_col")

      val ruleset = """Rules = [ColumnDataType "num_col" = "DOUBLE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail for DOUBLE when some values are invalid" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "11.5"),
        (2, "12.0"),
        (3, "not_a_number"),
        (4, "14.5")
      ).toDF("id", "num_col")

      val ruleset = """Rules = [ColumnDataType "num_col" = "DOUBLE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      metrics.values.head should be(0.75)
    }

    "pass for INTEGER when all values are valid integers" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "100"),
        (2, "200"),
        (3, "300")
      ).toDF("id", "int_col")

      val ruleset = """Rules = [ColumnDataType "int_col" = "INTEGER"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "pass for LONG when all values are valid" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "9999999999"),
        (2, "8888888888"),
        (3, "7777777777")
      ).toDF("id", "long_col")

      val ruleset = """Rules = [ColumnDataType "long_col" = "LONG"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "pass for FLOAT when all values are valid" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "1.5"),
        (2, "2.5"),
        (3, "3.5")
      ).toDF("id", "float_col")

      val ruleset = """Rules = [ColumnDataType "float_col" = "FLOAT"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }
  }

  "ColumnDataType for BOOLEAN type" should {

    "pass when all values are valid booleans" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "true"),
        (2, "false"),
        (3, "true")
      ).toDF("id", "bool_col")

      val ruleset = """Rules = [ColumnDataType "bool_col" = "BOOLEAN"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }
  }

  "ColumnDataType for DECIMAL type" should {

    "pass for DECIMAL(4,2) when values fit" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "7.5"),
        (2, "11.1"),
        (3, "56.78")
      ).toDF("id", "decimal_col")

      val ruleset = """Rules = [ColumnDataType "decimal_col" = "DECIMAL(4,2)"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "handle DECIMAL with spaces in definition" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "7.5"),
        (2, "11.1")
      ).toDF("id", "decimal_col")

      val ruleset = """Rules = [ColumnDataType "decimal_col" = "DECIMAL(4, 2)"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }
  }

  "ColumnDataType for TIMESTAMP type" should {

    "pass when all values are valid timestamps" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01 10:00:00"),
        (2, "2023-02-15 14:30:00"),
        (3, "2023-03-20 09:15:00")
      ).toDF("id", "ts_col")

      val ruleset = """Rules = [ColumnDataType "ts_col" = "TIMESTAMP"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }
  }

  "ColumnDataType with threshold" should {

    "pass when threshold is met" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "11.5"),
        (2, "12.0"),
        (3, "not_a_number"),
        (4, "14.5")
      ).toDF("id", "num_col")

      val ruleset = """Rules = [ColumnDataType "num_col" = "DOUBLE" with threshold >= 0.7]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail when threshold is not met" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "11.5"),
        (2, "not_a_number"),
        (3, "also_not"),
        (4, "14.5")
      ).toDF("id", "num_col")

      val ruleset = """Rules = [ColumnDataType "num_col" = "DOUBLE" with threshold >= 0.9]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }

    "pass with exact threshold match" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "11.5"),
        (2, "12.0"),
        (3, "not_a_number"),
        (4, "14.5")
      ).toDF("id", "num_col")

      val ruleset = """Rules = [ColumnDataType "num_col" = "DOUBLE" with threshold = 0.75]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }
  }

  "ColumnDataType with where clause" should {

    "work for rule with where clause" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "2023-02-15"),
        (3, "not-a-date"),
        (4, "2023-04-20")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" where "id <= 2"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      metrics.values.head should be(1.0)
    }

    "work for rule where all rows are filtered out" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "2023-02-15")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" where "id > 10"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[String]("FailureReason") should include("No rows matched")
    }

    "fail for rule with invalid where clause" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "2023-02-15")
      ).toDF("id", "date_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE" where "invalid%%clause"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("where clause is invalid")
    }
  }

  "ColumnDataType error handling" should {

    "fail for unrecognized data type" in withSparkSession { session =>
      import session.implicits._

      val df = Seq((1, "value")).toDF("id", "col")

      val ruleset = """Rules = [ColumnDataType "col" = "UNKNOWN_TYPE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("Unrecognized data type")
    }

    "fail when column does not exist" in withSparkSession { session =>
      import session.implicits._

      val df = Seq((1, "value")).toDF("id", "col")

      val ruleset = """Rules = [ColumnDataType "nonexistent" = "DATE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("does not exist")
    }
  }

  "ColumnDataType with NOT_EQUALS operator" should {

    "pass when values do NOT match the type" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "not-a-date"),
        (2, "also-not-a-date"),
        (3, "still-not-a-date")
      ).toDF("id", "col")

      val ruleset = """Rules = [ColumnDataType "col" != "DATE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
    }

    "fail when some values DO match the type" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01"),
        (2, "not-a-date"),
        (3, "2023-03-20")
      ).toDF("id", "col")

      val ruleset = """Rules = [ColumnDataType "col" != "DATE"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
    }
  }

  "ColumnDataType with multiple rules" should {

    "evaluate multiple ColumnDataType rules independently" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        (1, "2023-01-01", "100"),
        (2, "2023-02-15", "200"),
        (3, "2023-03-20", "not-a-number")
      ).toDF("id", "date_col", "int_col")

      val ruleset = """Rules = [ColumnDataType "date_col" = "DATE", ColumnDataType "int_col" = "INTEGER"]"""
      val results = EvaluateDataQuality.process(df, ruleset)

      val rows = results.collect()
      rows.length should be(2)

      val dateRule = rows.find(_.getAs[String]("Rule").contains("date_col")).get
      dateRule.getAs[String]("Outcome") should be("Passed")

      val intRule = rows.find(_.getAs[String]("Rule").contains("int_col")).get
      intRule.getAs[String]("Outcome") should be("Failed")
    }
  }
}
