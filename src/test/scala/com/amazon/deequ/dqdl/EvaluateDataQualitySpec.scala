/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

class EvaluateDataQualitySpec extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "DQDL ruleset" should {

    "run successfully on a given spark DataFrame" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[RowCount < 10]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      results.schema.fields.map(_.name) should contain allOf(
        "Rule",
        "Outcome",
        "FailureReason",
        "EvaluatedMetrics",
        "EvaluatedRule"
      )
      results.collect().length should be(1)

    }

    "support RowCount rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[RowCount < 10]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain("Dataset.*.RowCount" -> 4.0)
    }

    "support Completeness rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[Completeness \"item\" > 0.8]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain("Column.item.Completeness" -> 1.0)
    }

    "support IsComplete rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[IsComplete \"item\"]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain("Column.item.Completeness" -> 1.0)
    }

    "support Uniqueness rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[Uniqueness \"item\" = 1.0]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain("Column.item.Uniqueness" -> 1.0)
    }

    "support IsUnique rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[IsUnique \"item\"]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain("Column.item.Uniqueness" -> 1.0)
    }

    "support ColumnCorrelation rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)
      val ruleset = "Rules=[ColumnCorrelation \"att2\" \"att3\" > 0.8]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      // check the metric name
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Multicolumn.att2,att3.ColumnCorrelation")
      // check the correlation value
      (row.getAs[Map[String, Double]]("EvaluatedMetrics").values.toSeq.head * 100).toInt should be(99)
    }

    "support DistinctValuesCount rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)
      val ruleset = "Rules=[DistinctValuesCount \"att2\" = 4]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      // check the metric name
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.DistinctValuesCount")
      // check the metric value
      (row.getAs[Map[String, Double]]("EvaluatedMetrics").values.toSeq.head).toInt should be(4)
    }

    "support Entropy rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)
      val ruleset = "Rules=[Entropy \"att2\" > 1]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      // check the metric name
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.Entropy")
      // check the metric value
      (row.getAs[Map[String, Double]]("EvaluatedMetrics").values.toSeq.head * 100).toInt should be(124)
    }

    "support Mean rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)
      val ruleset = "Rules=[Mean \"att2\" = 3]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      // check the metric name
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.Mean")
      // check the metric value
      (row.getAs[Map[String, Double]]("EvaluatedMetrics").values.toSeq.head).toInt should be(3)
    }

    "support StandardDeviation rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)
      val ruleset = "Rules=[StandardDeviation \"att2\" > 3]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      // check the metric name
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.StandardDeviation")
      // check the metric value
      (row.getAs[Map[String, Double]]("EvaluatedMetrics").values.toSeq.head * 100).toInt should be(305)
    }

    "support Sum rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)
      val ruleset = "Rules=[Sum \"att2\" = 18]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      // check the metric name
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.Sum")
      // check the metric value
      row.getAs[Map[String, Double]]("EvaluatedMetrics").values.toSeq.head.toInt should be(18)
    }

    "support UniqueValueRatio rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)
      val ruleset = "Rules=[UniqueValueRatio \"att2\" > 0.7]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      // check the metric name
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.UniqueValueRatio")
      // check the metric value
      (row.getAs[Map[String, Double]]("EvaluatedMetrics").values.toSeq.head * 100).toInt should be(75)
    }

    "work with not yet supported rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      // Rule is not yet supported
      val ruleset = "Rules=[ColumnValues \"Foo\" = 5]"

      // when
      val resultDf = EvaluateDataQuality.process(df, ruleset)

      // then
      resultDf.collect()(0).getAs[String]("Outcome") should be("Failed")
      resultDf.collect()(0).getAs[String]("FailureReason") should be("Rule (or nested rule) not supported due to: " +
        "No converter found for rule type: ColumnValues")
    }

    "support CustomSql rule when Passed" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)
      df.createOrReplaceTempView("primary")

      val ruleset = "Rules=[CustomSql \"select count(*) from primary\" > 0, " +
        "CustomSql \"select count(*) from primary where att1 > 1 \" = 5, " +
        "CustomSql \"select count(*) from primary where att1 > 1 \" between 0 and 6]"

      // when
      val resultDf = EvaluateDataQuality.process(df, ruleset)

      // then
      val rows = resultDf.collect()
      rows.foreach { row =>
        row.getAs[String]("Outcome") should be("Passed")
        row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Dataset.*.CustomSQL")
        row.getAs[Map[String, Double]]("EvaluatedMetrics").seq.size should be(1)
      }

    }

    "support CustomSql rule when Failed" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      df.createOrReplaceTempView("primary")

      val ruleset = "Rules=[CustomSql \"select count(*) from primary\" > 4]"

      // when
      val resultDf = EvaluateDataQuality.process(df, ruleset)

      // then
      resultDf.collect()(0).getAs[String]("Outcome") should be("Failed")
      resultDf.collect()(0).getAs[String]("FailureReason") should
        be("Value: 4.0 does not meet the constraint requirement!")
    }

    "support both types: deequ and custom rules" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      df.createOrReplaceTempView("primary")
      val ruleset = "Rules=[RowCount < 10, CustomSql \"select count(*) from primary\" > 0]"

      // when
      val resultDf = EvaluateDataQuality.process(df, ruleset)

      // then
      resultDf.collect()(0).getAs[String]("Outcome") should be("Passed")
      resultDf.collect()(1).getAs[String]("Outcome") should be("Passed")
    }

    "support IsPrimaryKey rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)

      val ruleset = "Rules=[IsPrimaryKey \"item\"]"

      // when
      val resultDf = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = resultDf.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics").seq.size should be(2)
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.item.Uniqueness")
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.item.Completeness")
    }

    "support IsPrimaryKey rule when failed" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)

      val ruleset = "Rules=[IsPrimaryKey \"att2\"]"

      // when
      val resultDf = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = resultDf.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics").seq.size should be(2)
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.Uniqueness")
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.Completeness")
    }

    "support IsPrimaryKey rule with where clause" in withSparkSession { sparkSession =>
      // given
      val df = getDfWithNumericValues(sparkSession)

      val ruleset = "Rules=[IsPrimaryKey \"att2\" where \"att1 > 2\"]"

      // when
      val resultDf = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = resultDf.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics").seq.size should be(2)
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.Uniqueness")
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.Completeness")
    }

    "support ColumnLength rule with GREATER_THAN" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[ColumnLength \"item\" > 0]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain key "Column.item.MinimumLength"
    }

    "support ColumnLength rule with GREATER_THAN when failed" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[ColumnLength \"item\" > 5]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain key "Column.item.MinimumLength"
    }

    "support ColumnLength rule with LESS_THAN" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[ColumnLength \"item\" < 10]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain key "Column.item.MaximumLength"
    }

    "support ColumnLength rule with BETWEEN" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[ColumnLength \"item\" between 0 and 10]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      metrics should contain key "Column.item.MinimumLength"
      metrics should contain key "Column.item.MaximumLength"
    }

    "support ColumnLength rule with EQUALS" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[ColumnLength \"item\" = 1]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      metrics should contain key "Column.item.MinimumLength"
      metrics should contain key "Column.item.MaximumLength"
    }

    "support ColumnLength rule with IN" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[ColumnLength \"item\" in [1]]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      metrics should contain key "Column.item.LengthCompliance"
    }

    "support ColumnLength rule with NOT IN" in withSparkSession { sparkSession =>
      // given
      val df = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(sparkSession)
      val ruleset = "Rules=[ColumnLength \"item\" not in [7]]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      metrics should contain key "Column.item.LengthCompliance"
    }

    "support ColumnLength rule with where clause" in withSparkSession { sparkSession =>
      // given
      val df = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(sparkSession)
      val ruleset = "Rules=[ColumnLength \"item\" < 4 where \"val1 < 4\"]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain key "Column.item.MaximumLength"
    }

    "support ColumnLength rule between with where clause" in withSparkSession { sparkSession =>
      // given
      val df = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(sparkSession)
      val ruleset = "Rules=[ColumnLength \"item\" between 1 and 4 where \"val1 > 1 and val1 < 4\"]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain key "Column.item.MaximumLength"
    }

    "support ColumnExists rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(sparkSession)
      val ruleset = "Rules=[ColumnExists \"item\"]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain key "Dataset.item.ColumnExists"
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain value 1.0
    }

    "support ColumnExists rule fail" in withSparkSession { sparkSession =>
      // given
      val df = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(sparkSession)
      val ruleset = "Rules=[ColumnExists \"sampom\"]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain key "Dataset.sampom.ColumnExists"
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain value 0.0
    }

    "support multiple ColumnExists rule" in withSparkSession { sparkSession =>
      val df = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(sparkSession)

      val testOutcomes = Map(
        "sampom" -> 0.0,
        "item" -> 1.0,
        "val1" -> 1.0,
        "pomerantz" -> 0.0
      )

      testOutcomes.foreach { case (columnName, expectedMetric) =>
        val ruleset = s"""Rules=[ColumnExists "$columnName"]"""
        val results = EvaluateDataQuality.process(df, ruleset)
        val metrics = results.collect()(0).getAs[Map[String, Double]]("EvaluatedMetrics")
        metrics should contain key s"Dataset.$columnName.ColumnExists"
        metrics should contain value expectedMetric
      }
    }

    "support RowCountMatch rule" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq(
        ("1", "Alice"),
        ("2", "Bob"),
        ("3", "Charlie"),
        ("4", "Joshua Z")
      ).toDF("id", "name")

      val referenceDF = Seq(
        ("1", "Dave"),
        ("2", "Eve"),
        ("3", "Frank"),
        ("4", "Grace"),
        ("5", "Henry"),
        ("6", "Ivy"),
        ("7", "Jack")
      ).toDF("id", "name")

      val additionalDataSources = Map("ref" -> referenceDF)
      val ruleset = """Rules=[RowCountMatch "ref" >= 0.5]"""

      val results = EvaluateDataQuality.process(primaryDF, ruleset, additionalDataSources)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
      metrics should contain key "Dataset.ref.RowCountMatch"
      metrics("Dataset.ref.RowCountMatch") should be(4.0 / 7.0 +- 0.01)
    }

    "support RowCountMatch rule with between" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq(
        ("1", "Alice"),
        ("2", "Bob"),
        ("3", "Charlie"),
        ("4", "Joshua Z")
      ).toDF("id", "name")

      val referenceDF = Seq(
        ("1", "Dave"),
        ("2", "Eve"),
        ("3", "Frank"),
        ("4", "Grace"),
        ("5", "Henry"),
        ("6", "Ivy"),
        ("7", "Jack")
      ).toDF("id", "name")

      val additionalDataSources = Map("ref" -> referenceDF)
      val ruleset = """Rules=[RowCountMatch "ref" between 0.5 and 0.6]"""

      val results = EvaluateDataQuality.process(primaryDF, ruleset, additionalDataSources)

      results.collect()(0).getAs[String]("Outcome") should be("Passed")
    }

    "support RowCountMatch rule with not between" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq(
        ("1", "Alice"),
        ("2", "Joshua Z")
      ).toDF("id", "name")

      val referenceDF = Seq(
        ("1", "Bob"),
        ("2", "Charlie"),
        ("3", "Dave"),
        ("4", "Eve")
      ).toDF("id", "name")

      val additionalDataSources = Map("ref" -> referenceDF)
      val ruleset = """Rules=[RowCountMatch "ref" not between 0.8 and 0.9]"""

      val results = EvaluateDataQuality.process(primaryDF, ruleset, additionalDataSources)

      results.collect()(0).getAs[String]("Outcome") should be("Passed")
    }

    "support RowCountMatch rule when failed" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq(("1", "a"), ("2", "b")).toDF("id", "value")
      val referenceDF = Seq(("1", "a"), ("2", "b"), ("3", "c"), ("4", "d")).toDF("id", "value")

      val additionalDataSources = Map("ref" -> referenceDF)
      val ruleset = """Rules=[RowCountMatch "ref" = 1.0]"""

      val results = EvaluateDataQuality.process(primaryDF, ruleset, additionalDataSources)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("does not meet the constraint requirement")
    }

    "support RowCountMatch rule when reference not found" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq(("1", "a")).toDF("id", "value")
      val ruleset = """Rules=[RowCountMatch "missing" >= 0.5]"""

      val results = EvaluateDataQuality.process(primaryDF, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("not found in additional data sources")
    }

    "support ReferentialIntegrity rule" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      // All primary values exist in reference - 100% match
      val primaryDF = Seq("CA", "NY").toDF("state")
      val referenceDF = Seq("CA", "NY", "FL").toDF("state_code")

      val ruleset = """Rules=[ReferentialIntegrity "state" "ref.state_code" > 0.95]"""
      val results = EvaluateDataQuality.process(primaryDF, ruleset, Map("ref" -> referenceDF))

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain key "Column.ref.ReferentialIntegrity"
      row.getAs[Map[String, Double]]("EvaluatedMetrics")("Column.ref.ReferentialIntegrity") should be(1.0)
    }

    "support ReferentialIntegrity rule with partial match" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      // 3 out of 4 match = 75%
      val primaryDF = Seq(
        ("California", "CA"),
        ("New York", "NY"),
        ("New York", "NY"),
        ("Texas", "TX")  // TX not in reference
      ).toDF("State Name", "State Abbreviation")

      val referenceDF = Seq("CA", "NY", "FL").toDF("State Abbreviation")

      val ruleset = """Rules=[ReferentialIntegrity "State Abbreviation" "ref.State Abbreviation" > 0.6]"""
      val results = EvaluateDataQuality.process(primaryDF, ruleset, Map("ref" -> referenceDF))

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics")("Column.ref.ReferentialIntegrity") should be(0.75)
    }

    "support ReferentialIntegrity rule when failed with stricter threshold" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      // 3 out of 4 match = 75%, but threshold is 90%
      val primaryDF = Seq(
        ("California", "CA"),
        ("New York", "NY"),
        ("New York", "NY"),
        ("Texas", "TX")
      ).toDF("State Name", "State Abbreviation")

      val referenceDF = Seq("CA", "NY", "FL").toDF("State Abbreviation")

      val ruleset = """Rules=[ReferentialIntegrity "State Abbreviation" "ref.State Abbreviation" > 0.9]"""
      val results = EvaluateDataQuality.process(primaryDF, ruleset, Map("ref" -> referenceDF))

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("does not meet the constraint requirement")
      row.getAs[Map[String, Double]]("EvaluatedMetrics")("Column.ref.ReferentialIntegrity") should be(0.75)
    }

    "support ReferentialIntegrity rule with multiple columns" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq(
        ("Canada", "CA"),    // Incorrect - Canada/CA combo not in reference
        ("New York", "NY")   // Correct
      ).toDF("State Name", "State Abbreviation")

      val referenceDF = Seq(
        ("California", "CA"),
        ("New York", "NY"),
        ("Texas", "TX")
      ).toDF("State Name", "State Abbreviation")

      // 1 out of 2 match = 50%
      val ruleset =
        """Rules=[ReferentialIntegrity "State Name,State Abbreviation" """ +
        """"ref.{State Name,State Abbreviation}" > 0.4]"""
      val results = EvaluateDataQuality.process(primaryDF, ruleset, Map("ref" -> referenceDF))

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics")("Column.ref.ReferentialIntegrity") should be(0.5)
    }

    "support ReferentialIntegrity rule when reference not found" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq("CA").toDF("state")
      val ruleset = """Rules=[ReferentialIntegrity "state" "missing.state_code" >= 0.9]"""

      val results = EvaluateDataQuality.process(primaryDF, ruleset)

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("not found in additional sources")
    }

    "support ReferentialIntegrity rule when column not found in primary" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq(("California", "CA")).toDF("State Name", "State Abbreviation")
      val referenceDF = Seq("CA", "NY").toDF("State Abbreviation")

      val ruleset = """Rules=[ReferentialIntegrity "NonExistentColumn" "ref.State Abbreviation" > 0.9]"""
      val results = EvaluateDataQuality.process(primaryDF, ruleset, Map("ref" -> referenceDF))

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("does not exist")
    }

    "support ReferentialIntegrity rule when column not found in reference" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq("CA", "NY").toDF("state")
      val referenceDF = Seq("California", "New York").toDF("state_name")

      val ruleset = """Rules=[ReferentialIntegrity "state" "ref.NonExistentColumn" > 0.9]"""
      val results = EvaluateDataQuality.process(primaryDF, ruleset, Map("ref" -> referenceDF))

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Failed")
      row.getAs[String]("FailureReason") should include("does not exist")
    }

    "support ReferentialIntegrity with different column names" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val primaryDF = Seq("CA", "NY", "TX").toDF("state_abbr")
      val referenceDF = Seq("CA", "NY", "TX", "FL").toDF("abbreviation")

      val ruleset = """Rules=[ReferentialIntegrity "state_abbr" "ref.abbreviation" = 1.0]"""
      val results = EvaluateDataQuality.process(primaryDF, ruleset, Map("ref" -> referenceDF))

      val row = results.collect()(0)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics")("Column.ref.ReferentialIntegrity") should be(1.0)
    }

    "support ColumnNamesMatchPattern - pattern 'col_.*' matches all columns" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(("a", "b")).toDF("col_one", "col_two")

        val results = EvaluateDataQuality.process(df, """Rules=[ColumnNamesMatchPattern "col_.*"]""")

        val row = results.collect()(0)
        row.getAs[String]("Outcome") should be("Passed")
        val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
        metrics("Dataset.*.ColumnNamesPatternMatchRatio") should be(1.0)
      }

    "support ColumnNamesMatchPattern - pattern 'col_.*' fails for 'other'" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(("a", "b", "c")).toDF("col_one", "col_two", "other")

        val results = EvaluateDataQuality.process(df, """Rules=[ColumnNamesMatchPattern "col_.*"]""")

        val row = results.collect()(0)
        row.getAs[String]("Outcome") should be("Failed")
        row.getAs[String]("FailureReason") should include("other")
      }

    "support ColumnNamesMatchPattern - pattern 'Province.*' matches zero columns" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(("a", "b")).toDF("State Name", "State Abbreviation")

        val results = EvaluateDataQuality.process(df, """Rules=[ColumnNamesMatchPattern "Province.*"]""")

        val row = results.collect()(0)
        row.getAs[String]("Outcome") should be("Failed")
        val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
        metrics("Dataset.*.ColumnNamesPatternMatchRatio") should be(0.0)
      }

    "support ColumnNamesMatchPattern - pattern 'Building[\\s|_|\\.]Code'" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(("a", "b", "c")).toDF("Building Code", "Building_Code", "Building.Code")

        val rule = "ColumnNamesMatchPattern \"Building[\\s|_|\\.]Code\""
        val results = EvaluateDataQuality.process(df, s"Rules = [ $rule ]")

        val row = results.collect()(0)
        row.getAs[String]("Outcome") should be("Passed")
        val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
        metrics("Dataset.*.ColumnNamesPatternMatchRatio") should be(1.0)
      }

    "support ColumnNamesMatchPattern - pattern 'Building\\s*Code' partial match" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(("a", "b", "c")).toDF("Building Code", "Building_Code", "Building.Code")

        val rule = "ColumnNamesMatchPattern \"Building\\s*Code\""
        val results = EvaluateDataQuality.process(df, s"Rules = [ $rule ]")

        val row = results.collect()(0)
        row.getAs[String]("Outcome") should be("Failed")
        row.getAs[String]("FailureReason") should include("Building_Code")
        row.getAs[String]("FailureReason") should include("Building.Code")
      }

    "support ColumnNamesMatchPattern - invalid regex throws IllegalArgumentException" in
      withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = Seq(("a", "b")).toDF("col_one", "col_two")

        val rule = """ColumnNamesMatchPattern "[invalid(""""
        val ex = the [IllegalArgumentException] thrownBy {
          EvaluateDataQuality.process(df, s"Rules = [ $rule ]")
        }
        ex.getMessage should include("Invalid regex pattern")
        ex.getMessage should include("[invalid(")
      }

    "support ColumnNamesMatchPattern - empty dataframe returns Passed with NaN metric" in
      withSparkSession { sparkSession =>
        val df = sparkSession.createDataFrame(
          sparkSession.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          org.apache.spark.sql.types.StructType(Seq())
        )

        val results = EvaluateDataQuality.process(df, """Rules=[ColumnNamesMatchPattern "col_.*"]""")

        val row = results.collect()(0)
        row.getAs[String]("Outcome") should be("Passed")
        val metrics = row.getAs[Map[String, Double]]("EvaluatedMetrics")
        metrics("Dataset.*.ColumnNamesPatternMatchRatio").isNaN should be(true)
      }
  }

}
