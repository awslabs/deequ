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

  "DQDQ ruleset" should {

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

      val row = results.collect()(0)

      row.getAs[String]("Rule") should be("RowCount < 10")
      row.getAs[String]("Outcome") should be("Passed")
      row.getAs[String]("FailureReason") should be(null)
      row.getAs[Map[String, Double]]("EvaluatedMetrics") should contain("Dataset.*.RowCount" -> 4.0)

    }

    "support RowCount rule" in withSparkSession { sparkSession =>
      // given
      val df = getDfFull(sparkSession)
      val ruleset = "Rules=[RowCount < 10]"

      // when
      val results = EvaluateDataQuality.process(df, ruleset)

      // then
      results.collect()(0).getAs[String]("Outcome") should be("Passed")
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
