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

class CustomSqlRowLevelSpec extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "CustomSql row-level rule" should {

    "pass when all rows match" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", "Alice"), ("2", "Bob"), ("3", "Charlie")
      ).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT id, name FROM primary WHERE name IS NOT NULL"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      results.collect()(0).getAs[String]("Outcome") should be("Passed")
    }

    "fail when not all rows match" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", Some("Alice")), ("2", None), ("3", Some("Charlie"))
      ).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT id, name FROM primary WHERE name IS NOT NULL"]"""
      val results = EvaluateDataQuality.processRows(df, ruleset)

      val ruleOutcomes = results(EvaluateDataQuality.RULE_OUTCOMES_KEY)
      ruleOutcomes.collect()(0).getAs[String]("Outcome") should be("Failed")
      ruleOutcomes.collect()(0).getAs[Map[String, Double]]("EvaluatedMetrics")(
        "Dataset.*.CustomSQL.Compliance") should be(2.0 / 3.0 +- 0.01)

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select("DataQualityEvaluationResult")
        .collect().map(_.getString(0))
      outcomes should be(Array("Passed", "Failed", "Passed"))
    }

    "pass with threshold when enough rows match" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", Some("Alice")), ("2", None), ("3", Some("Charlie"))
      ).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT id, name FROM primary WHERE name IS NOT NULL" with threshold > 0.5]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      results.collect()(0).getAs[String]("Outcome") should be("Passed")
    }

    "fail with threshold when not enough rows match" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", Some("Alice")), ("2", None), ("3", Some("Charlie"))
      ).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT id, name FROM primary WHERE name IS NOT NULL" with threshold > 0.9]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      results.collect()(0).getAs[String]("Outcome") should be("Failed")
    }

    "still route scalar CustomSql through Deequ path" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", "Alice"), ("2", "Bob")
      ).toDF("id", "name")
      df.createOrReplaceTempView("primary")

      val ruleset = """Rules=[CustomSql "SELECT COUNT(*) FROM primary" > 0]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      results.collect()(0).getAs[String]("Outcome") should be("Passed")
    }

    "fail on invalid SQL" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("1", "Alice")).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT nonexistent FROM primary"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      results.collect()(0).getAs[String]("Outcome") should be("Failed")
    }

    "fail when SQL returns columns not in primary" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("1", "Alice")).toDF("id", "name")
      val ref = Seq(("1", "extra")).toDF("id", "other_col")
      ref.createOrReplaceTempView("ref")

      val ruleset = """Rules=[CustomSql "SELECT id, other_col FROM ref"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("not found")
    }

    "fail on empty DataFrame" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq.empty[(String, String)].toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT id, name FROM primary"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      results.collect()(0).getAs[String]("Outcome") should be("Failed")
    }

    "work with multiple columns in SQL result" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", "Alice", 25), ("2", "Bob", 30), ("3", "Charlie", 35)
      ).toDF("id", "name", "age")

      val ruleset = """Rules=[CustomSql "SELECT id, name, age FROM primary WHERE age >= 30"]"""
      val results = EvaluateDataQuality.processRows(df, ruleset)

      val ruleOutcomes = results(EvaluateDataQuality.RULE_OUTCOMES_KEY)
      ruleOutcomes.collect()(0).getAs[String]("Outcome") should be("Failed")
      ruleOutcomes.collect()(0).getAs[Map[String, Double]]("EvaluatedMetrics")(
        "Dataset.*.CustomSQL.Compliance") should be(2.0 / 3.0 +- 0.01)

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select("DataQualityEvaluationResult")
        .collect().map(_.getString(0))
      outcomes should be(Array("Failed", "Passed", "Passed"))
    }

    "coexist with scalar CustomSql in same ruleset" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", "Alice"), ("2", "Bob")
      ).toDF("id", "name")
      df.createOrReplaceTempView("primary")

      val ruleset =
        """Rules=[CustomSql "SELECT COUNT(*) FROM primary" > 0, """ +
        """CustomSql "SELECT id, name FROM primary WHERE name IS NOT NULL"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      val rows = results.collect()
      rows.length should be(2)
      rows.foreach(_.getAs[String]("Outcome") should be("Passed"))
    }

    "handle NULL values in SQL result correctly" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", Some("Alice")), ("2", None), ("3", Some("Charlie"))
      ).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT id, name FROM primary"]"""
      val results = EvaluateDataQuality.processRows(df, ruleset)

      val ruleOutcomes = results(EvaluateDataQuality.RULE_OUTCOMES_KEY)
      ruleOutcomes.collect()(0).getAs[String]("Outcome") should be("Passed")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select("DataQualityEvaluationResult")
        .collect().map(_.getString(0))
      outcomes should be(Array("Passed", "Passed", "Passed"))
    }

    "handle NULL values in join columns" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", Some("Alice")), ("2", None), ("3", Some("Charlie"))
      ).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT id, name FROM primary WHERE id != '2'"]"""
      val results = EvaluateDataQuality.processRows(df, ruleset)

      val ruleOutcomes = results(EvaluateDataQuality.RULE_OUTCOMES_KEY)
      ruleOutcomes.collect()(0).getAs[String]("Outcome") should be("Failed")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select("DataQualityEvaluationResult")
        .collect().map(_.getString(0))
      outcomes should be(Array("Passed", "Failed", "Passed"))
    }

    "fail with error when reference table column not in primary" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("1", "Alice"), ("2", "Bob")).toDF("id", "name")
      val ref = Seq(("1", 100), ("2", 200)).toDF("id", "amount")
      ref.createOrReplaceTempView("ref_table")

      val ruleset = """Rules=[CustomSql "SELECT id, amount FROM ref_table"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("not found")
    }

    "fail when joining ref table and selecting non-primary columns" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("1", "Alice"), ("2", "Bob")).toDF("id", "name")
      val ref = Seq(("1", 100), ("2", 200)).toDF("id", "amount")
      ref.createOrReplaceTempView("ref_join")

      val ruleset =
        """Rules=[CustomSql "SELECT ref_join.id, ref_join.amount """ +
        """FROM primary JOIN ref_join ON primary.id = ref_join.id"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("not found")
    }

    "handle column that exists in both primary and reference" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("1", "Alice"), ("2", "Bob")).toDF("id", "name")
      val ref = Seq(("1", "X"), ("2", "Y")).toDF("id", "code")
      ref.createOrReplaceTempView("ref_shared")

      val ruleset =
        """Rules=[CustomSql "SELECT primary.id FROM primary JOIN ref_shared ON primary.id = ref_shared.id"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      results.collect()(0).getAs[String]("Outcome") should be("Passed")
    }

    "handle non-primary-key selection with duplicates" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", "Alice"), ("2", "Alice"), ("3", "Bob"), ("4", "Bob")
      ).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT name FROM primary WHERE name = 'Alice'"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      val compliance = results.collect()(0).getAs[Map[String, Double]]("EvaluatedMetrics")(
        "Dataset.*.CustomSQL.Compliance")
      compliance should be > 0.5
    }

    "handle duplicate rows meeting condition" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", "Alice"), ("1", "Alice"), ("2", "Bob")
      ).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT id, name FROM primary WHERE name = 'Alice'"]"""
      val results = EvaluateDataQuality.process(df, ruleset)
      val compliance = results.collect()(0).getAs[Map[String, Double]]("EvaluatedMetrics")(
        "Dataset.*.CustomSQL.Compliance")
      compliance should be > 1.0 - 0.01
    }

    "return row-level results for processRows" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", Some("Alice")), ("2", None), ("3", Some("Charlie"))
      ).toDF("id", "name")

      val ruleset = """Rules=[CustomSql "SELECT id, name FROM primary WHERE name IS NOT NULL"]"""
      val results = EvaluateDataQuality.processRows(df, ruleset)

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select("DataQualityEvaluationResult")
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Failed", "Passed"))
    }
  }
}
