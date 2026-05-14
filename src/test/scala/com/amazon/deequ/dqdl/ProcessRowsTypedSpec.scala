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
import org.apache.spark.sql.types.BooleanType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProcessRowsTypedSpec extends AnyWordSpec with Matchers with SparkContextSpec {

  "processRowsTyped" should {

    "return outcomes map with correct rule count" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), ("Bob", 30), (null, 35)).toDF("name", "age")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ IsComplete "name", RowCount > 2 ]""")
      result.outcomes.size shouldBe 2
    }

    "return rowLevelData with boolean columns beyond original" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), ("Bob", 30)).toDF("name", "age")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ IsComplete "name" ]""")
      result.rowLevelData.columns.filterNot(df.columns.contains).foreach { col =>
        result.rowLevelData.schema(col).dataType shouldBe BooleanType
      }
    }

    "return rowLevelOutcomes with Pass/Fail/Skip columns" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), (null, 30)).toDF("name", "age")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ IsComplete "name" ]""")
      result.rowLevelOutcomes.columns should contain allOf(
        RowLevelResultHelper.ROW_LEVEL_PASS,
        RowLevelResultHelper.ROW_LEVEL_FAIL,
        RowLevelResultHelper.ROW_LEVEL_SKIP)
    }

    "return Failed outcome for failing rule" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), (null, 30)).toDF("name", "age")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ IsComplete "name" ]""")
      result.outcomes.values.head.outcome.asString shouldBe "Failed"
    }

    "handle composite rules" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), ("Bob", 30)).toDF("name", "age")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ (IsComplete "name") and (RowCount > 1) ]""")
      result.outcomes should not be empty
    }

    "throw on empty ruleset" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25)).toDF("name", "age")
      an[IllegalArgumentException] should be thrownBy {
        EvaluateDataQuality.processRowsTyped(df, """Rules = [ ]""")
      }
    }

    "return metrics for additional analyzers" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), ("Bob", 30), (null, 35)).toDF("name", "age")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ IsComplete "name" ]""",
        additionalAnalyzers = Seq(com.amazon.deequ.analyzers.Completeness("name")))
      result.metrics should not be empty
    }

    "handle additional analyzers with existing rules" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), ("Bob", 30)).toDF("name", "age")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ IsComplete "name" ]""",
        additionalAnalyzers = Seq(com.amazon.deequ.analyzers.Completeness("name")))
      result.outcomes should not be empty
      result.metrics should not be empty
    }

    "return Minimum and Maximum metrics for ColumnValues between rule" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 5), ("Dave", 50)).toDF("name", "age")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ ColumnValues "age" between 10 and 40 ]""")

      result.outcomes.values.head.outcome.asString shouldBe "Failed"

      val metricKeys = result.metrics.keys.map(_.toString).toSet
      metricKeys.exists(_.contains("Minimum")) shouldBe true
      metricKeys.exists(_.contains("Maximum")) shouldBe true

      val failureReason = result.outcomes.values.head.failureReason
      failureReason shouldBe defined
      failureReason.get should not include "% of rows"
    }

    "produce row-level results for ColumnDataType" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("123", 1), ("456", 2), ("abc", 3)).toDF("val", "id")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ ColumnDataType "val" = "Integral" with threshold > 0.5 ]""")
      result.outcomes should have size 1
      result.outcomes.values.head.outcome.asString shouldBe "Passed"
      result.rowLevelData.columns.length should be > df.columns.length
    }

    "produce FailureReason for failing ColumnDataType" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("abc", 1), ("def", 2), ("123", 3)).toDF("val", "id")
      val result = EvaluateDataQuality.processRowsTyped(df,
        """Rules = [ ColumnDataType "val" = "Integral" with threshold > 0.9 ]""")
      result.outcomes.values.head.outcome.asString shouldBe "Failed"
      val reason = result.outcomes.values.head.failureReason
      reason shouldBe defined
      reason.get should include("% of rows passed the threshold")
    }

    "produce distinct outcomes for multiple CustomSql rules" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), ("Bob", 30), ("Carol", 35))
        .toDF("name", "age")
      val ruleset = """Rules = [
        CustomSql "SELECT count(*) FROM primary WHERE age > 20" > 2,
        CustomSql "SELECT count(*) FROM primary WHERE age > 30" > 0
      ]"""
      val result = EvaluateDataQuality.processRowsTyped(df, ruleset)
      result.outcomes should have size 2
      result.outcomes.values.forall(
        _.outcome.asString == "Passed") shouldBe true
    }

    "handle mixed pass/fail in batched CustomSql" in withSparkSession { spark =>
      import spark.implicits._
      val df = Seq(("Alice", 25), ("Bob", 30)).toDF("name", "age")
      val ruleset = """Rules = [
        CustomSql "SELECT count(*) FROM primary WHERE age > 20" > 1,
        CustomSql "SELECT count(*) FROM primary WHERE age > 50" > 0
      ]"""
      val result = EvaluateDataQuality.processRowsTyped(df, ruleset)
      result.outcomes should have size 2
      val statuses = result.outcomes.values.map(
        _.outcome.asString).toSet
      statuses should contain("Passed")
      statuses should contain("Failed")
    }
  }
}
