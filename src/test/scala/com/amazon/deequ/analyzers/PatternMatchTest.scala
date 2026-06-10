/** Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
  */

package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.metrics.{DoubleMetric, FullColumn}
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PatternMatchTest
    extends AnyWordSpec
    with Matchers
    with SparkContextSpec
    with FixtureSupport {

  "PatternMatch" should {
    "return row-level results for non-null columns" in withSparkSession {
      session =>
        val data = getDfWithStringColumns(session)

        val patternMatchCountry = PatternMatch("Address Line 1", """\d""".r)
        val state = patternMatchCountry.computeStateFrom(data)
        val metric: DoubleMetric with FullColumn =
          patternMatchCountry.computeMetricFrom(state)

        data
          .withColumn("new", metric.fullColumn.get)
          .collect()
          .map(_.getAs[Any]("new")) shouldBe
          Seq(true, true, true, true, true, true, true, true)
    }

    "return row-level results for non-null columns starts with digit" in withSparkSession {
      session =>
        val data = getDfWithStringColumns(session)

        val patternMatchCountry =
          PatternMatch("Address Line 1", """(^[0-4])""".r)
        val state = patternMatchCountry.computeStateFrom(data)
        val metric: DoubleMetric with FullColumn =
          patternMatchCountry.computeMetricFrom(state)

        data
          .withColumn("new", metric.fullColumn.get)
          .collect()
          .map(_.getAs[Any]("new")) shouldBe
          Seq(false, false, true, true, false, false, true, true)
    }

    "return row-level results for non-null columns starts with digit filtered as true" in withSparkSession {
      session =>
        val data = getDfWithStringColumns(session)

        val patternMatchCountry = PatternMatch(
          "Address Line 1",
          """(^[0-4])""".r,
          where = Option("id < 5"),
          analyzerOptions =
            Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE))
        )
        val state = patternMatchCountry.computeStateFrom(data)
        val metric: DoubleMetric with FullColumn =
          patternMatchCountry.computeMetricFrom(state)

        data
          .withColumn("new", metric.fullColumn.get)
          .collect()
          .map(_.getAs[Any]("new")) shouldBe
          Seq(false, false, true, true, false, true, true, true)
    }

    "return row-level results for columns with nulls" in withSparkSession {
      session =>
        val data = getDfWithStringColumns(session)

        val patternMatchCountry = PatternMatch("Address Line 2", """\w""".r)
        val state = patternMatchCountry.computeStateFrom(data)
        val metric: DoubleMetric with FullColumn =
          patternMatchCountry.computeMetricFrom(state)

        data
          .withColumn("new", metric.fullColumn.get)
          .collect()
          .map(_.getAs[Any]("new")) shouldBe
          Seq(true, true, true, true, false, true, true, false)
    }

    "return row-level results for columns with nulls filtered as true" in withSparkSession {
      session =>
        val data = getDfWithStringColumns(session)

        val patternMatchCountry = PatternMatch(
          "Address Line 2",
          """\w""".r,
          where = Option("id < 5"),
          analyzerOptions =
            Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE))
        )
        val state = patternMatchCountry.computeStateFrom(data)
        val metric: DoubleMetric with FullColumn =
          patternMatchCountry.computeMetricFrom(state)

        data
          .withColumn("new", metric.fullColumn.get)
          .collect()
          .map(_.getAs[Any]("new")) shouldBe
          Seq(true, true, true, true, false, true, true, true)
    }

    "return row-level results for columns with nulls filtered as null" in withSparkSession {
      session =>
        val data = getDfWithStringColumns(session)

        val patternMatchCountry = PatternMatch(
          "Address Line 2",
          """\w""".r,
          where = Option("id < 5"),
          analyzerOptions =
            Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL))
        )
        val state = patternMatchCountry.computeStateFrom(data)
        val metric: DoubleMetric with FullColumn =
          patternMatchCountry.computeMetricFrom(state)

        data
          .withColumn("new", metric.fullColumn.get)
          .collect()
          .map(_.getAs[Any]("new")) shouldBe
          Seq(true, true, true, true, false, null, null, null)
    }

    "correctly identify valid and invalid US SSNs" in withSparkSession {
      session =>
        val data = getDfWithPatternMatch(session)

        val patternMatchSSN = PatternMatch(
          "SSN",
          SSN_US,
          analyzerOptions =
            Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE))
        )
        val state = patternMatchSSN.computeStateFrom(data)
        val metric: DoubleMetric with FullColumn =
          patternMatchSSN.computeMetricFrom(state)

        data
          .withColumn("SSN_Match", metric.fullColumn.get)
          .collect()
          .map(_.getAs[Any]("SSN_Match")) shouldBe
          Seq(true, true, false, false, false, false, false, true, true, false,
            true, false)
    }

    "correctly identify valid and invalid US phone numbers" in withSparkSession {
      session =>
        val data = getDfWithPatternMatch(session)

        val patternMatchPhone = PatternMatch(
          "PhoneNumber",
          PHONE_NUMBER_US,
          analyzerOptions =
            Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE))
        )
        val state = patternMatchPhone.computeStateFrom(data)
        val metric: DoubleMetric with FullColumn =
          patternMatchPhone.computeMetricFrom(state)

        data
          .withColumn("Phone_Match", metric.fullColumn.get)
          .collect()
          .map(_.getAs[Any]("Phone_Match")) shouldBe
          Seq(true, true, true, true, true, true, true, true, true, true, false,
            false)
    }

    "correctly identify valid and invalid US postal codes" in withSparkSession {
      session =>
        val data = getDfWithPatternMatch(session)

        val patternMatchPostalCode = PatternMatch(
          "PostalCode",
          POSTAL_CODE_US,
          analyzerOptions =
            Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE))
        )
        val state = patternMatchPostalCode.computeStateFrom(data)
        val metric: DoubleMetric with FullColumn =
          patternMatchPostalCode.computeMetricFrom(state)

        data
          .withColumn("PostalCode_Match", metric.fullColumn.get)
          .collect()
          .map(_.getAs[Any]("PostalCode_Match")) shouldBe
          Seq(true, true, true, true, true, false, true, false, false, true,
            true, false)
    }

  }
}
