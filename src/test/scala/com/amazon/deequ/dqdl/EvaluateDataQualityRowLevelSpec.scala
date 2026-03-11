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
import com.amazon.deequ.dqdl.execution.RowLevelResultHelper
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EvaluateDataQualityRowLevelSpec extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "processRows" should {

    "return all three result keys" in withSparkSession { spark =>
      val df = getDfFull(spark)
      val results = EvaluateDataQuality.processRows(df, """Rules=[IsComplete "item"]""")

      results should contain key EvaluateDataQuality.ORIGINAL_DATA_KEY
      results should contain key EvaluateDataQuality.RULE_OUTCOMES_KEY
      results should contain key EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY
    }

    "return originalData unchanged" in withSparkSession { spark =>
      val df = getDfFull(spark)
      val results = EvaluateDataQuality.processRows(df, """Rules=[IsComplete "item"]""")

      results(EvaluateDataQuality.ORIGINAL_DATA_KEY).collect() should be(df.collect())
    }

    "return correct ruleOutcomes" in withSparkSession { spark =>
      val df = getDfFull(spark)
      val results = EvaluateDataQuality.processRows(df, """Rules=[IsComplete "item"]""")

      val outcomes = results(EvaluateDataQuality.RULE_OUTCOMES_KEY)
      outcomes.count() should be(1)
      outcomes.collect()(0).getAs[String]("Outcome") should be("Passed")
    }

    "return row-level outcomes with correct schema" in withSparkSession { spark =>
      val df = getDfFull(spark)
      val results = EvaluateDataQuality.processRows(df, """Rules=[IsComplete "item"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      rowLevel.columns should contain allOf(
        RowLevelResultHelper.ROW_LEVEL_PASS,
        RowLevelResultHelper.ROW_LEVEL_FAIL,
        RowLevelResultHelper.ROW_LEVEL_SKIP,
        RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN
      )
    }

    "mark all rows as Passed for IsComplete on a complete column" in withSparkSession { spark =>
      val df = getDfFull(spark)
      val results = EvaluateDataQuality.processRows(df, """Rules=[IsComplete "item"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes.foreach(_ should be("Passed"))
      rowLevel.count() should be(4)
    }

    "mark rows as Failed for IsComplete on column with nulls" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), "a"),
        (None, "b"),
        (Some("3"), "c"),
        (None, "d")
      ).toDF("id", "value")

      val results = EvaluateDataQuality.processRows(df, """Rules=[IsComplete "id"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Failed", "Passed", "Failed"))
    }

    "populate DataQualityRulesPass and DataQualityRulesFail correctly" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), "a"),
        (None, "b")
      ).toDF("id", "value")

      val rule = """IsComplete "id""""
      val results = EvaluateDataQuality.processRows(df, s"""Rules=[$rule]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val rows = rowLevel.select(
        RowLevelResultHelper.ROW_LEVEL_PASS,
        RowLevelResultHelper.ROW_LEVEL_FAIL
      ).collect()

      rows(0).getAs[Seq[String]](0) should contain(rule)
      rows(0).getAs[Seq[String]](1) should be(empty)

      rows(1).getAs[Seq[String]](0) should be(empty)
      rows(1).getAs[Seq[String]](1) should contain(rule)
    }

    "handle multiple rules with mixed pass/fail" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), Some("a")),
        (None, Some("b")),
        (Some("3"), None)
      ).toDF("id", "name")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[IsComplete "id", IsComplete "name"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Failed", "Failed"))
    }

    "handle rules without row-level support by putting them in skip" in withSparkSession { spark =>
      val df = getDfFull(spark)
      val results = EvaluateDataQuality.processRows(df, """Rules=[RowCount > 0]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val rows = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_SKIP).collect()

      rows.foreach { row =>
        row.getAs[Seq[String]](0) should not be empty
      }
    }

    "handle composite rules" in withSparkSession { spark =>
      val df = getDfWithNumericValues(spark)
      val results = EvaluateDataQuality.processRows(df,
        """Rules=[(IsComplete "att2") and (IsComplete "att3")]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes.foreach(_ should be("Passed"))
    }

    "return same row count as input" in withSparkSession { spark =>
      val df = getDfWithNumericValues(spark)
      val results = EvaluateDataQuality.processRows(df,
        """Rules=[IsComplete "item", IsUnique "item", RowCount > 0]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      rowLevel.count() should be(df.count())
    }

    "preserve original data columns in row-level output" in withSparkSession { spark =>
      val df = getDfFull(spark)
      val results = EvaluateDataQuality.processRows(df, """Rules=[IsComplete "item"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      df.columns.foreach { col =>
        rowLevel.columns should contain(col)
      }
    }

    "work with additionalDataSources for RowCountMatch" in withSparkSession { spark =>
      import spark.implicits._
      val primary = Seq(("1", "a"), ("2", "b")).toDF("id", "value")
      val reference = Seq(("1", "x"), ("2", "y"), ("3", "z")).toDF("id", "value")

      val results = EvaluateDataQuality.processRows(
        primary,
        """Rules=[RowCountMatch "ref" >= 0.5]""",
        Map("ref" -> reference)
      )

      results should contain key EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY
      val ruleOutcomes = results(EvaluateDataQuality.RULE_OUTCOMES_KEY)
      ruleOutcomes.collect()(0).getAs[String]("Outcome") should be("Passed")
    }

    "return row-level results for ColumnValues IN rule" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (1, "a"),
        (2, "b"),
        (3, "c")
      ).toDF("id", "value")

      val rule = """ColumnValues "value" in ["a", "b"]"""
      val results = EvaluateDataQuality.processRows(df, s"""Rules=[$rule]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Passed", "Failed"))
    }

    "return row-level results for ColumnValues NOT IN rule" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (1, "a"),
        (2, "b"),
        (3, "c")
      ).toDF("id", "value")

      val rule = """ColumnValues "value" not in ["c"]"""
      val results = EvaluateDataQuality.processRows(df, s"""Rules=[$rule]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Passed", "Failed"))
    }

    "skip row-level for dataset-level rules like RowCount and Mean" in withSparkSession { spark =>
      val df = getDfWithNumericValues(spark)

      val results = EvaluateDataQuality.processRows(df, """Rules=[RowCount > 0, Mean "att2" > 0]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val skipped = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_SKIP).collect()

      skipped.foreach { row =>
        val skipList = row.getAs[Seq[String]](0)
        skipList.size should be(2)
      }
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))
      outcomes.foreach(_ should be("Passed"))
    }

    "handle mix of row-level and non-row-level rules" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some(1), "a"),
        (None, "b"),
        (Some(3), "c")
      ).toDF("id", "value")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[IsComplete "id", RowCount > 0]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val rows = rowLevel.select(
        RowLevelResultHelper.ROW_LEVEL_PASS,
        RowLevelResultHelper.ROW_LEVEL_FAIL,
        RowLevelResultHelper.ROW_LEVEL_SKIP
      ).collect()

      rows(0).getAs[Seq[String]](0) should contain("""IsComplete "id"""")
      rows(0).getAs[Seq[String]](2) should contain("RowCount > 0")

      rows(1).getAs[Seq[String]](1) should contain("""IsComplete "id"""")
      rows(1).getAs[Seq[String]](2) should contain("RowCount > 0")
    }

    "handle OR composite with row-level columns" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), Some("a")),
        (None, Some("b")),
        (Some("3"), None),
        (None, None)
      ).toDF("id", "name")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[(IsComplete "id") or (IsComplete "name")]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Passed", "Passed", "Failed"))
    }

    "handle AND composite with row-level columns" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), Some("a")),
        (None, Some("b")),
        (Some("3"), None),
        (None, None)
      ).toDF("id", "name")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[(IsComplete "id") and (IsComplete "name")]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Failed", "Failed", "Failed"))
    }

    "handle nested composite rules with row-level columns" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), Some("a"), Some("x")),
        (None, Some("b"), Some("y")),
        (None, None, Some("z"))
      ).toDF("id", "name", "code")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[(IsComplete "id") or ((IsComplete "name") and (IsComplete "code"))]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Passed", "Failed"))
    }

    "handle multiple independent composites" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), Some("a")),
        (None, Some("b"))
      ).toDF("id", "name")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[(IsComplete "id") or (IsComplete "name"), (IsComplete "id") and (IsComplete "name")]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Failed"))
    }

    "handle empty DataFrame" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq.empty[(String, String)].toDF("id", "name")

      val results = EvaluateDataQuality.processRows(df, """Rules=[IsComplete "id"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      rowLevel.count() should be(0)
      rowLevel.columns should contain allOf(
        RowLevelResultHelper.ROW_LEVEL_PASS,
        RowLevelResultHelper.ROW_LEVEL_FAIL,
        RowLevelResultHelper.ROW_LEVEL_SKIP,
        RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN
      )
    }

    "handle all rules failing on all rows" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Option.empty[String], Option.empty[String]),
        (Option.empty[String], Option.empty[String])
      ).toDF("id", "name")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[IsComplete "id", IsComplete "name"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes.foreach(_ should be("Failed"))

      val failedRules = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_FAIL).collect()
      failedRules.foreach { row =>
        row.getAs[Seq[String]](0).size should be(2)
      }
    }

    "handle all rules being dataset-level (all skip)" in withSparkSession { spark =>
      val df = getDfWithNumericValues(spark)

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[RowCount > 0, Mean "att2" > 0, Sum "att3" > 0]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val skipped = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_SKIP).collect()

      skipped.foreach { row =>
        row.getAs[Seq[String]](0).size should be(3)
      }

      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))
      outcomes.foreach(_ should be("Passed"))
    }

    "handle composite with shared nested rules" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), Some("a"), Some("x")),
        (None, Some("b"), Some("y"))
      ).toDF("id", "name", "code")

      // Both composites share IsComplete "id"
      val results = EvaluateDataQuality.processRows(df,
        """Rules=[(IsComplete "id") or (IsComplete "name"), (IsComplete "id") and (IsComplete "code")]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      // Row 1: OR passes (true OR true), AND passes (true AND true) -> Passed
      // Row 2: OR passes (false OR true), AND fails (false AND true) -> Failed
      outcomes should be(Array("Passed", "Failed"))
    }

    "handle deeply nested composite (3 levels)" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), Some("a"), Some("x"), Some("p")),
        (None, Some("b"), Some("y"), Some("q")),
        (None, None, Some("z"), Some("r"))
      ).toDF("id", "name", "code", "flag")

      // ((A OR B) AND (C OR D))
      val results = EvaluateDataQuality.processRows(df,
        """Rules=[((IsComplete "id") or (IsComplete "name")) and ((IsComplete "code") or (IsComplete "flag"))]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      // Row 1: (true OR true) AND (true OR true) = true AND true = Passed
      // Row 2: (false OR true) AND (true OR true) = true AND true = Passed
      // Row 3: (false OR false) AND (true OR true) = false AND true = Failed
      outcomes should be(Array("Passed", "Passed", "Failed"))
    }

    "handle null values in composite rules" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), "a"),
        (None, "b")
      ).toDF("id", "value")

      // IsComplete produces true/false, IsUnique might produce null for non-unique
      val results = EvaluateDataQuality.processRows(df,
        """Rules=[(IsComplete "id") and (IsUnique "id")]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      rowLevel.count() should be(2)

      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      // Both rows should have outcomes (not crash on null)
      outcomes.length should be(2)
    }

    "handle DataFreshness with row-level results" in withSparkSession { spark =>
      import spark.implicits._
      val today = java.time.LocalDate.now()
      val df = Seq(
        ("1", today.minusDays(1).toString),
        ("2", today.minusDays(5).toString),
        ("3", today.toString)
      ).toDF("id", "date")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[DataFreshness "date" <= 3 days]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Failed", "Passed"))
    }

    "handle IsComplete with where clause" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), "a", "active"),
        (None, "b", "active"),
        (Some("3"), "c", "inactive"),
        (None, "d", "inactive")
      ).toDF("id", "name", "status")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[IsComplete "id" where "status = 'active'"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Failed", "Passed", "Passed"))
    }

    "handle ReferentialIntegrity rule" in withSparkSession { spark =>
      import spark.implicits._
      val primary = Seq(
        ("CA", "California"),
        ("TX", "Texas"),
        ("ZZ", "Invalid")
      ).toDF("code", "name")

      val reference = Seq("CA", "NY", "TX").toDF("state_code")

      val results = EvaluateDataQuality.processRows(
        primary,
        """Rules=[ReferentialIntegrity "code" "ref.state_code" >= 0.6]""",
        Map("ref" -> reference)
      )

      // ReferentialIntegrity produces overall outcome
      val ruleOutcomes = results(EvaluateDataQuality.RULE_OUTCOMES_KEY)
      ruleOutcomes.collect()(0).getAs[String]("Outcome") should be("Passed")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      rowLevel.count() should be(3)
    }

    "handle ColumnValues with NULL keyword" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some(1), "a"),
        (None, "b"),
        (Some(3), "c")
      ).toDF("id", "value")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[ColumnValues "id" != NULL]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Passed", "Failed", "Passed"))
    }

    "handle Uniqueness rule with duplicates" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        ("1", "a"),
        ("1", "b"),
        ("2", "c")
      ).toDF("id", "value")

      val results = EvaluateDataQuality.processRows(df, """Rules=[IsUnique "id"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes should be(Array("Failed", "Failed", "Passed"))
    }

    "handle where clause that filters all rows" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(
        (Some("1"), "active"),
        (Some("2"), "active")
      ).toDF("id", "status")

      val results = EvaluateDataQuality.processRows(df,
        """Rules=[IsComplete "id" where "status = 'inactive'"]""")

      val rowLevel = results(EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY)
      val outcomes = rowLevel.select(RowLevelResultHelper.ROW_LEVEL_OUTCOME_COLUMN)
        .collect().map(_.getString(0))

      outcomes.foreach(_ should be("Passed"))
    }

    "handle process() and processRows() called sequentially" in withSparkSession { spark =>
      val df = getDfFull(spark)

      val aggregateResults = EvaluateDataQuality.process(df, """Rules=[IsComplete "item"]""")
      aggregateResults.count() should be(1)

      val rowResults = EvaluateDataQuality.processRows(df, """Rules=[IsComplete "item"]""")
      rowResults should contain key EvaluateDataQuality.ROW_LEVEL_OUTCOMES_KEY

      aggregateResults.collect()(0).getAs[String]("Outcome") should be("Passed")
      rowResults(EvaluateDataQuality.RULE_OUTCOMES_KEY).collect()(0).getAs[String]("Outcome") should be("Passed")
    }
  }
}
