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
import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * Verifies behavioral parity between Deequ's DQDL rule converters
 * and AwsGlueMlDataQualityETL's DQRuleTranslator for edge cases
 * involving NULLs, where clauses, and keyword operands.
 */
class AnalyzerOptionParitySpec extends AnyWordSpec
  with Matchers with SparkContextSpec with FixtureSupport {

  private def outcomeOf(df: DataFrame, ruleset: String): String = {
    val results = EvaluateDataQuality.process(df, ruleset)
    results.collect().head.getAs[String]("Outcome")
  }

  private def outcomesOf(df: DataFrame, ruleset: String): Seq[String] = {
    val results = EvaluateDataQuality.process(df, ruleset)
    results.collect().map(_.getAs[String]("Outcome")).toSeq
  }

  "ColumnLength" should {

    "treat NULL as length 0 via NullBehavior.EmptyString" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // EmptyString: NULL -> length 0, so min = 0, fails >= 2
        // Without EmptyString: NULL ignored, min = 2, would pass
        val df = Seq(
          (1, Some("ab")), (2, Some("abc")), (3, None)
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnLength "val" >= 2]""") should be("Failed")
      }

    "treat NULL as length 0 with where clause" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(
          (1, "a", Some("ab")), (2, "a", None), (3, "b", Some("x"))
        ).toDF("id", "grp", "val")

        outcomeOf(df,
          """Rules=[ColumnLength "val" >= 2 where "grp = 'a'"]"""
        ) should be("Failed")
      }
  }

  "ColumnValues numeric EQUALS" should {

    "fail when NULLs present via NullBehavior.Fail" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // NullBehavior.Fail: NULL -> extreme value in min/max
        val df = Seq(
          (1, Some(5)), (2, Some(5)), (3, None)
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" = 5]""") should be("Failed")
      }

    "pass when no NULLs and all values match" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(
          (1, 5), (2, 5), (3, 5)
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" = 5]""") should be("Passed")
      }
  }

  "Entropy" should {

    "compute over all rows ignoring where clause" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // grp='a': val='x','x','x' -> entropy=0 (single distinct value)
        // all rows: val='x','x','x','y','z' -> entropy > 0
        // threshold > 0.0: passes only if entropy > 0 (i.e. all rows used)
        // If WHERE were applied (only grp='a'), entropy=0, would fail
        val df = Seq(
          (1, "a", "x"), (2, "a", "x"), (3, "a", "x"),
          (4, "b", "y"), (5, "b", "z")
        ).toDF("id", "grp", "val")

        outcomeOf(df,
          """Rules=[Entropy "val" > 0.0 where "grp = 'a'"]"""
        ) should be("Passed")
      }
  }

  "ColumnValues string EMPTY keyword" should {

    "not match NULL rows via NULL guard" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // SQL: (val IS NOT NULL AND val = '')
        // Only row 1 matches -> 1/3 compliance -> fails
        val df = Seq(
          (1, Some("")), (2, None), (3, Some("hello"))
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" in [EMPTY]]"""
        ) should be("Failed")
      }

    "include NULL as passing in negated condition" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // SQL: (val IS NULL OR val != '')
        // Row 1 ("") fails, row 2 (null) passes, row 3 passes
        // -> 2/3 compliance -> fails default threshold 1.0
        val df = Seq(
          (1, Some("")), (2, None), (3, Some("hello"))
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" not in [EMPTY]]"""
        ) should be("Failed")
      }
  }

  "ColumnValues string WHITESPACES_ONLY keyword" should {

    "include NULL as passing in negated condition" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // SQL: (val IS NULL OR LENGTH(TRIM(val)) > 0 OR LENGTH(val) = 0)
        // Row 1 ("  ") fails, row 2 (null) passes, row 3 passes
        // -> 2/3 compliance -> fails default threshold 1.0
        val df = Seq(
          (1, Some("  ")), (2, None), (3, Some("hello"))
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" not in [WHITESPACES_ONLY]]"""
        ) should be("Failed")
      }
  }

  "Where clause filtering" should {

    "scope Completeness to matching rows only" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // grp='a': 3 rows, 1 null -> completeness 2/3
        val df = Seq(
          (1, "a", Some("x")), (2, "a", Some("y")),
          (3, "a", None), (4, "b", Some("z"))
        ).toDF("id", "grp", "val")

        outcomeOf(df,
          """Rules=[Completeness "val" > 0.5 where "grp = 'a'"]"""
        ) should be("Passed")

        outcomeOf(df,
          """Rules=[Completeness "val" > 0.8 where "grp = 'a'"]"""
        ) should be("Failed")
      }

    "scope IsPrimaryKey to matching rows only" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // val unique within grp='a' but duplicated across groups
        val df = Seq(
          (1, "a", "x"), (2, "a", "y"), (3, "b", "x")
        ).toDF("id", "grp", "val")

        outcomeOf(df,
          """Rules=[IsPrimaryKey "val" where "grp = 'a'"]"""
        ) should be("Passed")
      }

    "handle multiple rules with mixed where clauses" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(
          (1, "a", 10), (2, "a", 20),
          (3, "b", -5), (4, "b", 15)
        ).toDF("id", "grp", "val")

        val results = outcomesOf(df,
          """Rules=[ColumnValues "val" > 0 where "grp = 'a'",""" +
          """ IsComplete "id", RowCount > 2]""")

        results should have size 3
        results.foreach(_ should be("Passed"))
      }
  }

  "ColumnValues with threshold" should {

    "pass string IN when compliance exceeds threshold" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // 3 of 4 rows match -> 75% compliance
        // threshold > 0.5 -> should pass
        val df = Seq(
          (1, "active"), (2, "active"), (3, "active"),
          (4, "deleted")
        ).toDF("id", "status")

        outcomeOf(df,
          """Rules=[ColumnValues "status" in ["active"] """ +
          """with threshold > 0.5]"""
        ) should be("Passed")
      }

    "fail string IN when compliance below threshold" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // 3 of 4 rows match -> 75% compliance
        // threshold > 0.8 -> should fail
        val df = Seq(
          (1, "active"), (2, "active"), (3, "active"),
          (4, "deleted")
        ).toDF("id", "status")

        outcomeOf(df,
          """Rules=[ColumnValues "status" in ["active"] """ +
          """with threshold > 0.8]"""
        ) should be("Failed")
      }

    "pass matches when compliance exceeds threshold" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // 3 of 4 rows match pattern -> 75%
        val df = Seq(
          (1, "abc"), (2, "def"), (3, "ghi"), (4, "123")
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" matches "[a-z]+" """ +
          """with threshold > 0.5]"""
        ) should be("Passed")
      }

    "pass with threshold between range" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // 3 of 4 match -> 75%
        // threshold between 0.5 and 0.9 -> 0.75 is in range
        val df = Seq(
          (1, "active"), (2, "active"), (3, "active"),
          (4, "deleted")
        ).toDF("id", "status")

        outcomeOf(df,
          """Rules=[ColumnValues "status" in ["active"] """ +
          """with threshold between 0.5 and 0.9]"""
        ) should be("Passed")
      }

    "work with threshold and where clause together" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // grp='a': 3 rows, 2 match "active" -> 67%
        // threshold > 0.5 -> should pass
        val df = Seq(
          (1, "a", "active"), (2, "a", "active"),
          (3, "a", "deleted"), (4, "b", "deleted")
        ).toDF("id", "grp", "status")

        outcomeOf(df,
          """Rules=[ColumnValues "status" in ["active"] """ +
          """where "grp = 'a'" with threshold > 0.5]"""
        ) should be("Passed")
      }

    "match case-insensitively with IGNORE_CASE tag" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // "Active" and "ACTIVE" should match "active" with IGNORE_CASE
        val df = Seq(
          (1, "Active"), (2, "ACTIVE"), (3, "active"),
          (4, "deleted")
        ).toDF("id", "status")

        outcomeOf(df,
          """Rules=[ColumnValues "status" in ["active"] """ +
          """with IGNORE_CASE = "true" with threshold > 0.5]"""
        ) should be("Passed")
      }

    "apply IGNORE_CASE only to quoted strings, not keywords" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // NULL rows should still be handled by keyword logic, not lowercased
        val df = Seq(
          (1, Some("Active")), (2, Some("ACTIVE")),
          (3, None), (4, Some("deleted"))
        ).toDF("id", "status")

        // in ["active", NULL] with IGNORE_CASE: rows 1,2,3 match -> 75%
        outcomeOf(df,
          """Rules=[ColumnValues "status" in ["active", NULL] """ +
          """with IGNORE_CASE = "true" with threshold > 0.5]"""
        ) should be("Passed")
      }
  }

  "IsPrimaryKey multi-column" should {

    "scope composite key check to matching rows via where clause" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // (col1, col2) unique within grp='a' but duplicated across groups
        val df = Seq(
          (1, "a", "x", 1), (2, "a", "y", 2),
          (3, "b", "x", 1)
        ).toDF("id", "grp", "col1", "col2")

        outcomeOf(df,
          """Rules=[IsPrimaryKey "col1" "col2" where "grp = 'a'"]"""
        ) should be("Passed")
      }
  }

  "IGNORE_CASE with NOT_IN" should {

    "match case-insensitively for negated set membership" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // "BANNED" matches "banned" with IGNORE_CASE -> not in set -> fails
        val df = Seq(
          (1, "ok"), (2, "BANNED"), (3, "fine")
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" not in ["banned"] """ +
          """with IGNORE_CASE = "true"]"""
        ) should be("Failed")
      }
  }

  "IGNORE_CASE with EQUALS and NOT_EQUALS" should {

    "match case-insensitively for string equality" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(
          (1, "Hello"), (2, "HELLO"), (3, "hello")
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" = "hello" """ +
          """with IGNORE_CASE = "true"]"""
        ) should be("Passed")
      }

    "match case-insensitively for string not-equals" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // All values are case-variants of "hello" -> all equal -> not-equals fails
        val df = Seq(
          (1, "Hello"), (2, "HELLO"), (3, "hello")
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" != "hello" """ +
          """with IGNORE_CASE = "true"]"""
        ) should be("Failed")
      }
  }

  "ColumnValues numeric EQUALS with WHERE and NULLs" should {

    "fail when NULLs exist in filtered subset" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // grp='a': values 5, 5, NULL -> NULL triggers Fail behavior
        val df = Seq(
          (1, "a", Some(5)), (2, "a", Some(5)),
          (3, "a", None), (4, "b", Some(9))
        ).toDF("id", "grp", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" = 5 where "grp = 'a'"]"""
        ) should be("Failed")
      }
  }

  "All-NULL column" should {

    "fail ColumnLength when entire column is NULL" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // All NULLs -> EmptyString makes all lengths 0 -> fails >= 1
        val df = Seq(
          (1, None: Option[String]), (2, None: Option[String])
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnLength "val" >= 1]""") should be("Failed")
      }

    "fail ColumnValues EQUALS when entire column is NULL" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // All NULLs -> NullBehavior.Fail -> fails
        val df = Seq(
          (1, None: Option[Int]), (2, None: Option[Int])
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" = 5]""") should be("Failed")
      }
  }

  "Metric value assertion" should {

    "return correct Completeness metric value" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // 2 of 3 non-null -> completeness = 0.6667
        val df = Seq(
          (1, Some("a")), (2, Some("b")), (3, None)
        ).toDF("id", "val")

        val metrics = metricsOf(df,
          """Rules=[Completeness "val" > 0.5]""")
        val completeness = metrics.values.head
        completeness should be(0.667 +- 0.01)
      }
  }

  "IGNORE_CASE tag variants" should {

    "accept uppercase TRUE value" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(
          (1, "Hello"), (2, "HELLO"), (3, "hello")
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" in ["hello"] """ +
          """with IGNORE_CASE = "TRUE"]"""
        ) should be("Passed")
      }
  }

  "Threshold edge case" should {

    "pass with > 0.0 threshold when any row matches" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // 1 of 3 matches -> 33% > 0.0 -> passes
        val df = Seq(
          (1, "yes"), (2, "no"), (3, "no")
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" in ["yes"] """ +
          """with threshold > 0.0]"""
        ) should be("Passed")
      }
  }

  "Numeric NOT_BETWEEN with threshold" should {

    "respect threshold instead of requiring 100% compliance" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // 3 of 4 rows are NOT between 10 and 20 -> 75% compliance
        // threshold > 0.5 -> should pass (would fail with hardcoded _ == 1.0)
        val df = Seq(
          (1, 5), (2, 15), (3, 25), (4, 30)
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" not between 10 and 20 """ +
          """with threshold > 0.5]"""
        ) should be("Passed")
      }

    "fail when compliance below threshold" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // 1 of 4 rows is NOT between 0 and 25 -> 25% compliance
        // threshold > 0.5 -> should fail
        val df = Seq(
          (1, 5), (2, 15), (3, 20), (4, 30)
        ).toDF("id", "val")

        outcomeOf(df,
          """Rules=[ColumnValues "val" not between 0 and 25 """ +
          """with threshold > 0.5]"""
        ) should be("Failed")
      }
  }

  "Date rules with WHERE clause" should {

    "scope date comparison to matching rows" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        // grp='a': dates are 2024-01-15, 2024-01-20 -> both > 2024-01-01
        // grp='b': date is 2023-06-01 -> would fail > 2024-01-01
        val df = Seq(
          (1, "a", "2024-01-15"), (2, "a", "2024-01-20"),
          (3, "b", "2023-06-01")
        ).toDF("id", "grp", "dt")

        outcomeOf(df,
          """Rules=[ColumnValues "dt" > "2024-01-01" where "grp = 'a'"]"""
        ) should be("Passed")
      }
  }

  "Empty DataFrame" should {

    "not throw for ColumnLength with zero rows" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq.empty[(Int, String)].toDF("id", "val")
        // Empty DF: no rows to evaluate, outcome depends on analyzer defaults
        noException should be thrownBy outcomeOf(df,
          """Rules=[ColumnLength "val" >= 1]""")
      }

    "not throw for ColumnValues EQUALS with zero rows" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq.empty[(Int, Int)].toDF("id", "val")
        noException should be thrownBy outcomeOf(df,
          """Rules=[ColumnValues "val" = 5]""")
      }

    "not throw for IsPrimaryKey with zero rows" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq.empty[(Int, String)].toDF("id", "val")
        noException should be thrownBy outcomeOf(df,
          """Rules=[IsPrimaryKey "val"]""")
      }
  }

  private def metricsOf(df: DataFrame, ruleset: String): Map[String, Double] = {
    val results = EvaluateDataQuality.process(df, ruleset)
    results.collect().head.getAs[Map[String, Double]]("EvaluatedMetrics")
  }
}
