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
      print(row)
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[Map[String, Double]]("EvaluatedMetrics").keys should contain("Column.att2.UniqueValueRatio")
      // check the metric value
      (row.getAs[Map[String, Double]]("EvaluatedMetrics").values.toSeq.head * 100).toInt should be(75)
    }

    "work with not yet supported rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      // CustomSql is not yet supported
      val ruleset = "Rules=[CustomSql \"select count(*) from primary\" between 10 and 20]"

      // when
      val resultDf = EvaluateDataQuality.process(df, ruleset)

      // then
      resultDf.collect()(0).getAs[String]("Outcome") should be("Failed")
      resultDf.collect()(0).getAs[String]("FailureReason") should be("Rule (or nested rule) not supported due to: " +
        "No converter found for rule type: CustomSql")
    }

  }

}
