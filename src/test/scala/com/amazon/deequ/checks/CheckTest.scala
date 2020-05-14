/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  *     http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ
package checks

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.anomalydetection.{Anomaly, AnomalyDetectionStrategy}
import com.amazon.deequ.constraints.{ConstrainableDataTypes, ConstraintStatus}
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Success, Try}

class CheckTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport
  with MockFactory {

  import CheckTest._

  "Check" should {

    "return the correct check status for completeness" in withSparkSession { sparkSession =>

      val check1 = Check(CheckLevel.Error, "group-1")
        .isComplete("att1") // 1.0
        .hasCompleteness("att1", _ == 1.0) // 1.0

      val check2 = Check(CheckLevel.Error, "group-2-E")
        .hasCompleteness("att2", _ > 0.8) // 0.75

      val check3 = Check(CheckLevel.Warning, "group-2-W")
        .hasCompleteness("att2", _ > 0.8) // 0.75

      val context = runChecks(getDfCompleteAndInCompleteColumns(sparkSession),
        check1, check2, check3)

      context.metricMap.foreach { println }

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Error)
      assertEvaluatesTo(check3, context, CheckStatus.Warning)
    }

    "return the correct check status for combined completeness" in
      withSparkSession { sparkSession =>

        val check1 = Check(CheckLevel.Error, "group-1")
          .areComplete(Seq("item", "att1")) // 1.0
          .haveCompleteness(Seq("item", "att1"), _ == 1.0) // 1.0

        val check2 = Check(CheckLevel.Error, "group-2-E")
          .haveCompleteness(Seq("item", "att1", "att2"), _ > 0.8) // 0.75

        val check3 = Check(CheckLevel.Warning, "group-2-W")
          .haveCompleteness(Seq("item", "att1", "att2"), _ > 0.8) // 0.75

        val context = runChecks(getDfCompleteAndInCompleteColumns(sparkSession),
          check1, check2, check3)

        context.metricMap.foreach { println }

        assertEvaluatesTo(check1, context, CheckStatus.Success)
        assertEvaluatesTo(check2, context, CheckStatus.Error)
        assertEvaluatesTo(check3, context, CheckStatus.Warning)
    }

    "return the correct check status for any completeness" in
      withSparkSession { sparkSession =>

        val check1 = Check(CheckLevel.Error, "group-1")
          .areAnyComplete(Seq("item", "att1")) // 1.0
          .haveAnyCompleteness(Seq("item", "att1"), _ == 1.0) // 1.0

        val check2 = Check(CheckLevel.Error, "group-2-E")
          .haveAnyCompleteness(Seq("att1", "att2"), _ > 0.917) // 11/12 (0.91666)

        val check3 = Check(CheckLevel.Warning, "group-2-W")
          .haveAnyCompleteness(Seq("att1", "att2"), _ > 0.917) // 11/12 (0.91666)

        val context = runChecks(getDfMissing(sparkSession),
          check1, check2, check3)

        context.metricMap.foreach { println }

        assertEvaluatesTo(check1, context, CheckStatus.Success)
        assertEvaluatesTo(check2, context, CheckStatus.Error)
        assertEvaluatesTo(check3, context, CheckStatus.Warning)
      }

    "return the correct check status for uniqueness" in withSparkSession { sparkSession =>

      val check = Check(CheckLevel.Error, "group-1")
        .isUnique("unique")
        .isUnique("uniqueWithNulls")
        .isUnique("halfUniqueCombinedWithNonUnique").where("nonUnique > 0")
        .isUnique("nonUnique")
        .isUnique("nonUniqueWithNulls")

      val context = runChecks(getDfWithUniqueColumns(sparkSession), check)
      val result = check.evaluate(context)

      assert(result.status == CheckStatus.Error)
      val constraintStatuses = result.constraintResults.map(_.status)
      assert(constraintStatuses.head == ConstraintStatus.Success)
      assert(constraintStatuses(1) == ConstraintStatus.Success)
      assert(constraintStatuses(2) == ConstraintStatus.Success)

      assert(constraintStatuses(3) == ConstraintStatus.Failure)
      assert(constraintStatuses(4) == ConstraintStatus.Failure)
    }

    "return the correct check status for primary key" in withSparkSession { sparkSession =>

      val check = Check(CheckLevel.Error, "primary-key-check")
        .isPrimaryKey("unique")
        .isPrimaryKey("halfUniqueCombinedWithNonUnique", "onlyUniqueWithOtherNonUnique")
        .isPrimaryKey("halfUniqueCombinedWithNonUnique").where("nonUnique > 0")
        .isPrimaryKey("nonUnique", Some("hint"), "halfUniqueCombinedWithNonUnique")
        .where("nonUnique > 0")
        .isPrimaryKey("nonUnique")
        .isPrimaryKey("nonUnique", "nonUniqueWithNulls")

      val context = runChecks(getDfWithUniqueColumns(sparkSession), check)
      val result = check.evaluate(context)

      assert(result.status == CheckStatus.Error)
      val constraintStatuses = result.constraintResults.map(_.status)
      assert(constraintStatuses.head == ConstraintStatus.Success)
      assert(constraintStatuses(1) == ConstraintStatus.Success)
      assert(constraintStatuses(2) == ConstraintStatus.Success)
      assert(constraintStatuses(3) == ConstraintStatus.Success)

      assert(constraintStatuses(4) == ConstraintStatus.Failure)
      assert(constraintStatuses(5) == ConstraintStatus.Failure)
    }

    "return the correct check status for distinctness" in withSparkSession { sparkSession =>

      val check = Check(CheckLevel.Error, "distinctness-check")
        .hasDistinctness(Seq("att1"), _ == 3.0 / 5)
        .hasDistinctness(Seq("att1"), _ == 2.0 / 3).where("att2 is not null")
        .hasDistinctness(Seq("att1", "att2"), _ == 4.0 / 6)
        .hasDistinctness(Seq("att2"), _ == 1.0)

      val context = runChecks(getDfWithDistinctValues(sparkSession), check)
      val result = check.evaluate(context)

      assert(result.status == CheckStatus.Error)
      val constraintStatuses = result.constraintResults.map { _.status }
      assert(constraintStatuses.head == ConstraintStatus.Success)
      assert(constraintStatuses(1) == ConstraintStatus.Success)
      assert(constraintStatuses(2) == ConstraintStatus.Success)
      assert(constraintStatuses(3) == ConstraintStatus.Failure)
    }

    "return the correct check status for hasUniqueness" in withSparkSession { sparkSession =>

      val check = Check(CheckLevel.Error, "group-1-u")
        .hasUniqueness("nonUnique", (fraction: Double) => fraction == .5)
        .hasUniqueness("nonUnique", (fraction: Double) => fraction < .6)
        .hasUniqueness(Seq("halfUniqueCombinedWithNonUnique", "nonUnique"),
          (fraction: Double) => fraction == .5)
        .hasUniqueness(Seq("onlyUniqueWithOtherNonUnique", "nonUnique"), Check.IsOne)
        .hasUniqueness("unique", Check.IsOne)
        .hasUniqueness("uniqueWithNulls", Check.IsOne)
        .hasUniqueness(Seq("nonUnique", "halfUniqueCombinedWithNonUnique"), Check.IsOne)
        .where("nonUnique > 0")
        .hasUniqueness(Seq("nonUnique", "halfUniqueCombinedWithNonUnique"), Check.IsOne,
          Some("hint"))
        .where("nonUnique > 0")
        .hasUniqueness("halfUniqueCombinedWithNonUnique", Check.IsOne)
        .where("nonUnique > 0")
        .hasUniqueness("halfUniqueCombinedWithNonUnique", Check.IsOne, Some("hint"))
        .where("nonUnique > 0")

      val context = runChecks(getDfWithUniqueColumns(sparkSession), check)
      val result = check.evaluate(context)

      assert(result.status == CheckStatus.Success)
      val constraintStatuses = result.constraintResults.map { _.status }
      // Half of nonUnique column are duplicates
      assert(constraintStatuses.head == ConstraintStatus.Success)
      assert(constraintStatuses(1) == ConstraintStatus.Success)
      // Half of the 2 columns are duplicates as well.
      assert(constraintStatuses(2) == ConstraintStatus.Success)
      // Both next 2 cases are actually unique so should meet threshold
      assert(constraintStatuses(3) == ConstraintStatus.Success)
      assert(constraintStatuses(4) == ConstraintStatus.Success)
      // Nulls are duplicated so this will not be unique
      assert(constraintStatuses(5) == ConstraintStatus.Success)
      // Multi-column uniqueness, duplicates filtered out
      assert(constraintStatuses(6) == ConstraintStatus.Success)
      // Multi-column uniqueness with hint, duplicates filtered out
      assert(constraintStatuses(7) == ConstraintStatus.Success)
      // Single-column uniqueness, duplicates filtered out
      assert(constraintStatuses(8) == ConstraintStatus.Success)
      // Single-column uniqueness with hint, duplicates filtered out
      assert(constraintStatuses(9) == ConstraintStatus.Success)
    }

    "return the correct check status for hasUniqueValueRatio" in withSparkSession { sparkSession =>

      val check = Check(CheckLevel.Error, "unique-value-ratio-check")
        .hasUniqueValueRatio(Seq("nonUnique", "halfUniqueCombinedWithNonUnique"), _ == 0.75)
        .hasUniqueValueRatio(Seq("nonUnique", "halfUniqueCombinedWithNonUnique"), Check.IsOne)
        .where("nonUnique > 0")
        .hasUniqueValueRatio(Seq("nonUnique"), Check.IsOne, Some("hint"))
        .where("nonUnique > 0")

      val context = runChecks(getDfWithUniqueColumns(sparkSession), check)
      val result = check.evaluate(context)

      assert(result.status == CheckStatus.Success)
      val constraintStatuses = result.constraintResults.map { _.status }
      assert(constraintStatuses.head == ConstraintStatus.Success)
      assert(constraintStatuses(1) == ConstraintStatus.Success)
      assert(constraintStatuses(2) == ConstraintStatus.Success)
    }

    "return the correct check status for size" in withSparkSession { sparkSession =>
      val df = getDfCompleteAndInCompleteColumns(sparkSession)
      val numberOfRows = df.count()

      val check1 = Check(CheckLevel.Error, "group-1-S-1")
        .hasSize(_ == numberOfRows)

      val check2 = Check(CheckLevel.Warning, "group-1-S-2")
        .hasSize(_ == numberOfRows)

      val check3 = Check(CheckLevel.Error, "group-1-E")
        .hasSize(_ != numberOfRows)

      val check4 = Check(CheckLevel.Warning, "group-1-W")
        .hasSize(_ != numberOfRows)

      val check5 = Check(CheckLevel.Warning, "group-1-W-Range")
        .hasSize { size => size > 0 && size < numberOfRows + 1 }

      val context = runChecks(df, check1, check2, check3, check4, check5)

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Success)
      assertEvaluatesTo(check3, context, CheckStatus.Error)
      assertEvaluatesTo(check4, context, CheckStatus.Warning)
      assertEvaluatesTo(check5, context, CheckStatus.Success)
    }

    "return the correct check status for columns constraints" in withSparkSession { sparkSession =>

      val check1 = Check(CheckLevel.Error, "group-1")
        .satisfies("att1 > 0", "rule1")

      val check2 = Check(CheckLevel.Error, "group-2-to-fail")
        .satisfies("att1 > 3", "rule2")

      val check3 = Check(CheckLevel.Error, "group-2-to-succeed")
        .satisfies("att1 > 3", "rule3", _ == 0.5)

      val context = runChecks(getDfWithNumericValues(sparkSession), check1, check2, check3)

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Error)
      assertEvaluatesTo(check3, context, CheckStatus.Success)
    }

    "return the correct check status for conditional column constraints" in
      withSparkSession { sparkSession =>

        val checkToSucceed = Check(CheckLevel.Error, "group-1")
          .satisfies("att1 < att2", "rule1").where("att1 > 3")

        val checkToFail = Check(CheckLevel.Error, "group-1")
          .satisfies("att2 > 0", "rule2").where("att1 > 0")

        val checkPartiallyGetsSatisfied = Check(CheckLevel.Error, "group-1")
          .satisfies("att2 > 0", "rule3", _ == 0.5).where("att1 > 0")

        val context = runChecks(getDfWithNumericValues(sparkSession), checkToSucceed, checkToFail,
          checkPartiallyGetsSatisfied)

        assertEvaluatesTo(checkToSucceed, context, CheckStatus.Success)
        assertEvaluatesTo(checkToFail, context, CheckStatus.Error)
        assertEvaluatesTo(checkPartiallyGetsSatisfied, context, CheckStatus.Success)
      }

    "correctly evaluate less than constraints" in withSparkSession { sparkSession =>
      val lessThanCheck = Check(CheckLevel.Error, "a")
        .isLessThan("att1", "att2").where("item > 3")

      val incorrectLessThanCheck = Check(CheckLevel.Error, "a")
        .isLessThan("att1", "att2")

      val lessThanCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isLessThan("att1", "att2", _ == 0.5)

      val incorrectLessThanCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isLessThan("att1", "att2", _ == 0.4)

      val results = runChecks(getDfWithNumericValues(sparkSession), lessThanCheck,
        incorrectLessThanCheck, lessThanCheckWithCustomAssertionFunction,
        incorrectLessThanCheckWithCustomAssertionFunction)

      assertEvaluatesTo(lessThanCheck, results, CheckStatus.Success)
      assertEvaluatesTo(incorrectLessThanCheck, results, CheckStatus.Error)
      assertEvaluatesTo(lessThanCheckWithCustomAssertionFunction, results, CheckStatus.Success)
      assertEvaluatesTo(incorrectLessThanCheckWithCustomAssertionFunction, results,
        CheckStatus.Error)
    }

    "correctly evaluate less than or equal to constraints" in withSparkSession { sparkSession =>
      val lessThanOrEqualCheck = Check(CheckLevel.Error, "a")
        .isLessThanOrEqualTo("att1", "att3").where("item > 3")

      val incorrectLessThanOrEqualCheck = Check(CheckLevel.Error, "a")
        .isLessThanOrEqualTo("att1", "att3")

      val lessThanOrEqualCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isLessThanOrEqualTo("att1", "att3", _ == 0.5)

      val incorrectLessThanOrEqualCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isLessThanOrEqualTo("att1", "att3", _ == 0.4)

      val results = runChecks(getDfWithNumericValues(sparkSession), lessThanOrEqualCheck,
        incorrectLessThanOrEqualCheck, lessThanOrEqualCheckWithCustomAssertionFunction,
        incorrectLessThanOrEqualCheckWithCustomAssertionFunction)

      assertEvaluatesTo(lessThanOrEqualCheck, results, CheckStatus.Success)
      assertEvaluatesTo(incorrectLessThanOrEqualCheck, results, CheckStatus.Error)
      assertEvaluatesTo(lessThanOrEqualCheckWithCustomAssertionFunction, results,
        CheckStatus.Success)
      assertEvaluatesTo(incorrectLessThanOrEqualCheckWithCustomAssertionFunction, results,
        CheckStatus.Error)
    }

    "correctly evaluate greater than constraints" in withSparkSession { sparkSession =>
      val greaterThanCheck = Check(CheckLevel.Error, "a")
        .isGreaterThan("att2", "att1").where("item > 3")

      val incorrectGreaterThanCheck = Check(CheckLevel.Error, "a")
        .isGreaterThan("att2", "att1")

      val greaterThanCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isGreaterThan("att2", "att1", _ == 0.5)

      val incorrectGreaterThanCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isGreaterThan("att2", "att1", _ == 0.4)

      val results = runChecks(getDfWithNumericValues(sparkSession), greaterThanCheck,
        incorrectGreaterThanCheck, greaterThanCheckWithCustomAssertionFunction,
        incorrectGreaterThanCheckWithCustomAssertionFunction)

      assertEvaluatesTo(greaterThanCheck, results, CheckStatus.Success)
      assertEvaluatesTo(incorrectGreaterThanCheck, results, CheckStatus.Error)
      assertEvaluatesTo(greaterThanCheckWithCustomAssertionFunction, results, CheckStatus.Success)
      assertEvaluatesTo(incorrectGreaterThanCheckWithCustomAssertionFunction, results,
        CheckStatus.Error)
    }

    "correctly evaluate greater than or equal to constraints" in withSparkSession { sparkSession =>
      val greaterThanOrEqualCheck = Check(CheckLevel.Error, "a")
        .isGreaterThanOrEqualTo("att3", "att1").where("item > 3")

      val incorrectGreaterOrEqualThanCheck = Check(CheckLevel.Error, "a")
        .isGreaterThanOrEqualTo("att3", "att1")

      val greaterThanOrEqualCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isGreaterThanOrEqualTo("att3", "att1", _ == 0.5)

      val incorrectGreaterThanOrEqualCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isGreaterThanOrEqualTo("att3", "att1", _ == 0.4)

      val results = runChecks(getDfWithNumericValues(sparkSession), greaterThanOrEqualCheck,
        incorrectGreaterOrEqualThanCheck, greaterThanOrEqualCheckWithCustomAssertionFunction,
        incorrectGreaterThanOrEqualCheckWithCustomAssertionFunction)

      assertEvaluatesTo(greaterThanOrEqualCheck, results, CheckStatus.Success)
      assertEvaluatesTo(incorrectGreaterOrEqualThanCheck, results, CheckStatus.Error)
      assertEvaluatesTo(greaterThanOrEqualCheckWithCustomAssertionFunction, results,
        CheckStatus.Success)
      assertEvaluatesTo(incorrectGreaterThanOrEqualCheckWithCustomAssertionFunction, results,
        CheckStatus.Error)
    }

    "correctly evaluate non negative and positive constraints" in withSparkSession { sparkSession =>
      val nonNegativeCheck = Check(CheckLevel.Error, "a")
        .isNonNegative("item")

      val isPositiveCheck = Check(CheckLevel.Error, "a")
        .isPositive("item")

      val results = runChecks(getDfWithNumericValues(sparkSession), nonNegativeCheck,
        isPositiveCheck)

      assertEvaluatesTo(nonNegativeCheck, results, CheckStatus.Success)
      assertEvaluatesTo(isPositiveCheck, results, CheckStatus.Success)
    }

    "correctly evaluate range constraints" in withSparkSession { sparkSession =>
      val rangeCheck = Check(CheckLevel.Error, "a")
        .isContainedIn("att1", Array("a", "b", "c"))

      val inCorrectRangeCheck = Check(CheckLevel.Error, "a")
        .isContainedIn("att1", Array("a", "b"))

      val inCorrectRangeCheckWithCustomAssertionFunction = Check(CheckLevel.Error, "a")
        .isContainedIn("att1", Array("a"), _ == 0.5)

      val rangeResults = runChecks(getDfWithDistinctValues(sparkSession), rangeCheck,
        inCorrectRangeCheck, inCorrectRangeCheckWithCustomAssertionFunction)

      assertEvaluatesTo(rangeCheck, rangeResults, CheckStatus.Success)
      assertEvaluatesTo(inCorrectRangeCheck, rangeResults, CheckStatus.Error)
      assertEvaluatesTo(inCorrectRangeCheckWithCustomAssertionFunction, rangeResults,
        CheckStatus.Success)

      val numericRangeCheck1 = Check(CheckLevel.Error, "nr1")
        .isContainedIn("att2", 0, 7)

      val numericRangeCheck2 = Check(CheckLevel.Error, "nr2")
        .isContainedIn("att2", 1, 7)

      val numericRangeCheck3 = Check(CheckLevel.Error, "nr3")
        .isContainedIn("att2", 0, 6)

      val numericRangeCheck4 = Check(CheckLevel.Error, "nr4")
        .isContainedIn("att2", 0, 7, includeLowerBound = false, includeUpperBound = false)

      val numericRangeCheck5 = Check(CheckLevel.Error, "nr5")
          .isContainedIn("att2", -1, 8, includeLowerBound = false, includeUpperBound = false)

      val numericRangeCheck6 = Check(CheckLevel.Error, "nr6")
          .isContainedIn("att2", 0, 7, includeLowerBound = true, includeUpperBound = false)

      val numericRangeCheck7 = Check(CheckLevel.Error, "nr7")
          .isContainedIn("att2", 0, 8, includeLowerBound = true, includeUpperBound = false)

      val numericRangeCheck8 = Check(CheckLevel.Error, "nr8")
          .isContainedIn("att2", 0, 7, includeLowerBound = false, includeUpperBound = true)

      val numericRangeCheck9 = Check(CheckLevel.Error, "nr9")
          .isContainedIn("att2", -1, 7, includeLowerBound = false, includeUpperBound = true)

      val numericRangeResults = runChecks(getDfWithNumericValues(sparkSession), numericRangeCheck1,
        numericRangeCheck2, numericRangeCheck3, numericRangeCheck4, numericRangeCheck5,
        numericRangeCheck6, numericRangeCheck7, numericRangeCheck8, numericRangeCheck9)

      assertEvaluatesTo(numericRangeCheck1, numericRangeResults, CheckStatus.Success)
      assertEvaluatesTo(numericRangeCheck2, numericRangeResults, CheckStatus.Error)
      assertEvaluatesTo(numericRangeCheck3, numericRangeResults, CheckStatus.Error)
      assertEvaluatesTo(numericRangeCheck4, numericRangeResults, CheckStatus.Error)
      assertEvaluatesTo(numericRangeCheck5, numericRangeResults, CheckStatus.Success)
      assertEvaluatesTo(numericRangeCheck6, numericRangeResults, CheckStatus.Error)
      assertEvaluatesTo(numericRangeCheck7, numericRangeResults, CheckStatus.Success)
      assertEvaluatesTo(numericRangeCheck8, numericRangeResults, CheckStatus.Error)
      assertEvaluatesTo(numericRangeCheck9, numericRangeResults, CheckStatus.Success)
    }

    "return the correct check status for histogram constraints" in
      withSparkSession { sparkSession =>

        val check1 = Check(CheckLevel.Error, "group-1")
          .hasNumberOfDistinctValues("att1", _ < 10)
          .hasHistogramValues("att1", _ ("a").absolute == 4)
          .hasHistogramValues("att1", _ ("b").absolute == 2)
          .hasHistogramValues("att1", _ ("a").ratio > 0.6)
          .hasHistogramValues("att1", _ ("b").ratio < 0.4)
          .hasHistogramValues("att1", _ ("a").absolute == 3)
          .where("att2 is not null")
          .hasHistogramValues("att1", _ ("b").absolute == 1)
          .where("att2 is not null")

        val check2 = Check(CheckLevel.Error, "group-1")
          .hasNumberOfDistinctValues("att2", _ == 3)
          .hasNumberOfDistinctValues("att2", _ == 2).where("att1 = 'a'")
          .hasHistogramValues("att2", _ ("f").absolute == 3)
          .hasHistogramValues("att2", _ ("d").absolute == 1)
          .hasHistogramValues("att2", _ (Histogram.NullFieldReplacement).absolute == 2)
          .hasHistogramValues("att2", _ ("f").ratio == 3 / 6.0)
          .hasHistogramValues("att2", _ ("d").ratio == 1 / 6.0)
          .hasHistogramValues("att2", _ (Histogram.NullFieldReplacement).ratio == 2 / 6.0)

        val check3 = Check(CheckLevel.Error, "group-1")
          .hasNumberOfDistinctValues("unKnownColumn", _ == 3)

        val context = runChecks(getDfCompleteAndInCompleteColumns(sparkSession), check1,
          check2, check3)

        assertEvaluatesTo(check1, context, CheckStatus.Success)
        assertEvaluatesTo(check2, context, CheckStatus.Success)
        assertEvaluatesTo(check3, context, CheckStatus.Error)
      }

    "return the correct check status for entropy constraints" in withSparkSession { sparkSession =>

      val expectedValue = -(0.75 * math.log(0.75) + 0.25 * math.log(0.25))

      val check1 = Check(CheckLevel.Error, "group-1")
        .hasEntropy("att1", _ == expectedValue)

      val check2 = Check(CheckLevel.Error, "group-1")
        .hasEntropy("att1", _ == 0).where("att2 = 'c'")

      val check3 = Check(CheckLevel.Error, "group-1")
        .hasEntropy("att1", _ != expectedValue)

      val context = runChecks(getDfFull(sparkSession), check1, check2, check3)

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Success)
      assertEvaluatesTo(check3, context, CheckStatus.Error)
    }

    "return the correct check status for mutual information constraints" in
      withSparkSession { sparkSession =>

        val check = Check(CheckLevel.Error, "check")
          .hasMutualInformation("att1", "att2", _ === 0.5623 +- 0.0001)
        val checkWithFilter = Check(CheckLevel.Error, "check")
          .hasMutualInformation("att1", "att2", _ == 0).where("att2 = 'c'")

        val context = runChecks(getDfFull(sparkSession), check, checkWithFilter)

        assertEvaluatesTo(check, context, CheckStatus.Success)
        assertEvaluatesTo(checkWithFilter, context, CheckStatus.Success)
      }

    "yield correct results for basic stats" in withSparkSession { sparkSession =>
      val baseCheck = Check(CheckLevel.Error, description = "a description")
      val dfNumeric = getDfWithNumericValues(sparkSession)
      val dfInformative = getDfWithConditionallyInformativeColumns(sparkSession)
      val dfUninformative = getDfWithConditionallyUninformativeColumns(sparkSession)

      val numericAnalysis = Analysis().addAnalyzers(Seq(
        Minimum("att1"), Maximum("att1"), Mean("att1"), Sum("att1"),
        StandardDeviation("att1"), ApproxCountDistinct("att1"),
        ApproxQuantile("att1", quantile = 0.5)))

      val contextNumeric = numericAnalysis.run(dfNumeric)

      assertSuccess(baseCheck.hasMin("att1", _ == 1.0), contextNumeric)
      assertSuccess(baseCheck.hasMax("att1", _ == 6.0), contextNumeric)
      assertSuccess(baseCheck.hasMean("att1", _ == 3.5), contextNumeric)
      assertSuccess(baseCheck.hasSum("att1", _ == 21.0), contextNumeric)
      assertSuccess(baseCheck.hasStandardDeviation("att1", _ == 1.707825127659933), contextNumeric)
      assertSuccess(baseCheck.hasApproxCountDistinct("att1", _ == 6.0), contextNumeric)
      assertSuccess(baseCheck.hasApproxQuantile("att1", quantile = 0.5, _ == 3.0), contextNumeric)

      val correlationAnalysis = Analysis().addAnalyzer(Correlation("att1", "att2"))

      val contextInformative = correlationAnalysis.run(dfInformative)
      val contextUninformative = correlationAnalysis.run(dfUninformative)

      assertSuccess(baseCheck.hasCorrelation("att1", "att2", _ == 1.0), contextInformative)
      assertSuccess(baseCheck.hasCorrelation("att1", "att2", java.lang.Double.isNaN),
        contextUninformative)
    }

    "correctly evaluate mean constraints" in withSparkSession { sparkSession =>
      val meanCheck = Check(CheckLevel.Error, "a")
        .hasMean("att1", _ == 3.5)
      val meanCheckWithFilter = Check(CheckLevel.Error, "a")
        .hasMean("att1", _ == 5.0).where("att2 > 0")

      val context = runChecks(getDfWithNumericValues(sparkSession), meanCheck,
        meanCheckWithFilter)

      assertSuccess(meanCheck, context)
      assertSuccess(meanCheckWithFilter, context)
    }

    "correctly evaluate hasApproxQuantile constraints" in withSparkSession { sparkSession =>
      val hasApproxQuantileCheck = Check(CheckLevel.Error, "a")
        .hasApproxQuantile("att1", quantile = 0.5, _ == 3.0)
      val hasApproxQuantileCheckWithFilter = Check(CheckLevel.Error, "a")
        .hasApproxQuantile("att1", quantile = 0.5, _ == 5.0).where("att2 > 0")

      val context = runChecks(getDfWithNumericValues(sparkSession), hasApproxQuantileCheck,
        hasApproxQuantileCheckWithFilter)

      assertSuccess(hasApproxQuantileCheck, context)
      assertSuccess(hasApproxQuantileCheckWithFilter, context)
    }

    "yield correct results for minimum and maximum length stats" in
      withSparkSession { sparkSession =>
        val baseCheck = Check(CheckLevel.Error, description = "a description")
        val df = getDfWithVariableStringLengthValues(sparkSession)
        val context = AnalysisRunner.onData(df)
          .addAnalyzers(Seq(MinLength("att1"), MaxLength("att1"))).run()

        assertSuccess(baseCheck.hasMinLength("att1", _ == 0.0), context)
        assertSuccess(baseCheck.hasMaxLength("att1", _ == 4.0), context)
    }

    "work on regular expression patterns for E-Mails" in withSparkSession { sparkSession =>
      val col = "some"
      val df = dataFrameWithColumn(col, StringType, sparkSession, Row("someone@somewhere.org"),
        Row("someone@else.com"))
      val check = Check(CheckLevel.Error, "some description")
        .hasPattern(col, Patterns.EMAIL)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "fail on mixed data for E-Mail pattern with default assertion" in withSparkSession { session =>
      val col = "some"
      val df = dataFrameWithColumn(col, StringType, session, Row("someone@somewhere.org"),
        Row("someone@else"))
      val check = Check(CheckLevel.Error, "some description")
        .hasPattern(col, Patterns.EMAIL)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Error)
    }

    "work on regular expression patterns for URLs" in withSparkSession { sparkSession =>
      val col = "some"
      val df = dataFrameWithColumn(col, StringType, sparkSession,
        Row("https://www.example.com/foo/?bar=baz&inga=42&quux"), Row("https://foo.bar/baz"))
      val check = Check(CheckLevel.Error, "some description").hasPattern(col, Patterns.URL)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "work on regular expression patterns with filtering" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        ("someone@somewhere.org", "valid"),
        ("someone@else", "invalid")
      ).toDF("value", "type")

      val check = Check(CheckLevel.Error, "some description")
        .hasPattern("value", Patterns.EMAIL, _ == 0.5)
      val checkWithFilter = Check(CheckLevel.Error, "some description")
        .hasPattern("value", Patterns.EMAIL, _ == 1.0).where("type = 'valid'")

      val context = runChecks(df, check, checkWithFilter)

      assertEvaluatesTo(check, context, CheckStatus.Success)
      assertEvaluatesTo(checkWithFilter, context, CheckStatus.Success)
    }

    "fail on mixed data for URL pattern with default assertion" in withSparkSession {
      sparkSession =>
      val col = "some"
      val df = dataFrameWithColumn(col, StringType, sparkSession,
        Row("https://www.example.com/foo/?bar=baz&inga=42&quux"), Row("http:// shouldfail.com"))
      val check = Check(CheckLevel.Error, "some description").hasPattern(col, Patterns.URL)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Error)
    }

    "isCreditCard" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        ("4111 1111 1111 1111", "valid"),
        ("9999888877776666", "invalid")
      ).toDF("value", "type")

      val check = Check(CheckLevel.Error, "some description")
        .containsCreditCardNumber("value", _ == 0.5)
      val checkWithFilter = Check(CheckLevel.Error, "some description")
        .containsCreditCardNumber("value", _ == 1.0).where("type = 'valid'")

      val context = runChecks(df, check, checkWithFilter)

      assertEvaluatesTo(check, context, CheckStatus.Success)
      assertEvaluatesTo(checkWithFilter, context, CheckStatus.Success)
    }

    "define is E-Mail" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        ("someone@somewhere.org", "valid"),
        ("someone@else", "invalid")
      ).toDF("value", "type")

      val check = Check(CheckLevel.Error, "some description")
        .containsEmail("value", _ == 0.5)
      val checkWithFilter = Check(CheckLevel.Error, "some description")
        .containsEmail("value", _ == 1.0).where("type = 'valid'")

      val context = runChecks(df, check, checkWithFilter)

      assertEvaluatesTo(check, context, CheckStatus.Success)
      assertEvaluatesTo(checkWithFilter, context, CheckStatus.Success)
    }

    "define is US social security number" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        ("111-05-1130", "valid"),
        ("something else", "invalid")
      ).toDF("value", "type")

      val check = Check(CheckLevel.Error, "some description")
        .containsSocialSecurityNumber("value", _ == 0.5)
      val checkWithFilter = Check(CheckLevel.Error, "some description")
        .containsSocialSecurityNumber("value", _ == 1.0).where("type = 'valid'")

      val context = runChecks(df, check, checkWithFilter)

      assertEvaluatesTo(check, context, CheckStatus.Success)
      assertEvaluatesTo(checkWithFilter, context, CheckStatus.Success)
    }

    "define is URL" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        ("https://www.example.com/foo/?bar=baz&inga=42&quux", "valid"),
        ("http:// shouldfail.com", "invalid")
      ).toDF("value", "type")

      val check = Check(CheckLevel.Error, "some description")
        .containsURL("value", _ == 0.5)
      val checkWithFilter = Check(CheckLevel.Error, "some description")
        .containsURL("value", _ == 1.0).where("type = 'valid'")

      val context = runChecks(df, check, checkWithFilter)

      assertEvaluatesTo(check, context, CheckStatus.Success)
      assertEvaluatesTo(checkWithFilter, context, CheckStatus.Success)
    }

    "define has data type" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        ("2", "integral"),
        ("1.0", "fractional")
      ).toDF("value", "type")

      val check = Check(CheckLevel.Error, "some description")
        .hasDataType("value", ConstrainableDataTypes.Integral, _ == 0.5)
      val checkWithFilter = Check(CheckLevel.Error, "some description")
        .hasDataType("value", ConstrainableDataTypes.Integral, _ == 1.0).where("type = 'integral'")

      val context = runChecks(df, check, checkWithFilter)

      assertEvaluatesTo(check, context, CheckStatus.Success)
      assertEvaluatesTo(checkWithFilter, context, CheckStatus.Success)
    }

    "find credit card numbers embedded in text" in withSparkSession { sparkSession =>
      val col = "some"
      val df = dataFrameWithColumn(col, StringType, sparkSession,
        Row("My credit card number is: 4111-1111-1111-1111."))
      val check = Check(CheckLevel.Error, "some description")
        .containsCreditCardNumber(col, _ == 1.0)
      val context = runChecks(df, check)
      context.allMetrics.foreach(println)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "find E-mails embedded in text" in withSparkSession { sparkSession =>
      val col = "some"
      val df = dataFrameWithColumn(col, StringType, sparkSession,
        Row("Please contact me at someone@somewhere.org, thank you."))
      val check = Check(CheckLevel.Error, "some description").containsEmail(col, _ == 1.0)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "find URLs embedded in text" in withSparkSession { sparkSession =>
      val col = "some"
      val df = dataFrameWithColumn(col, StringType, sparkSession,
        Row("Hey, please have a look at https://www.example.com/foo/?bar=baz&inga=42&quux!"))
      val check = Check(CheckLevel.Error, "some description").containsURL(col, _ == 1.0)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "find SSNs embedded in text" in withSparkSession { sparkSession =>
      val col = "some"
      val df = dataFrameWithColumn(col, StringType, sparkSession,
        Row("My SSN is 111-05-1130, thanks."))
      val check = Check(CheckLevel.Error, "some description")
        .containsSocialSecurityNumber(col, _ == 1.0)
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Success)
    }

    "non negativity check works for numeric columns" in withSparkSession { sparkSession =>
      Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType).foreach { dataType =>
        assertNonNegativeCheckIsSuccessFor(dataType, sparkSession)
      }
    }

    "is positive check works for numeric columns" in withSparkSession { sparkSession =>
      Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType).foreach { dataType =>
        assertIsPositiveCheckIsSuccessFor(dataType, sparkSession)
      }
    }
  }

  "Check on column names with special characters" should {

    def testWithExoticColumnName(df: DataFrame, c: Check): Unit = {
      val r = VerificationSuite()
        .onData(df)
        .addCheck(c)
        .run()
      assert(r.status == CheckStatus.Success)
    }

    val valuesStr: Seq[ItemStr] = Seq(
      ItemStr("NULL"),
      ItemStr("NULL"),
      ItemStr("-10.0"),
      ItemStr("-10.0"),
      ItemStr("-10.0"),
      ItemStr("10.0"),
      ItemStr("0.0"),
      ItemStr("1.5245"),
      ItemStr("1.5245"),
      ItemStr("1.5245"),
      ItemStr("-4.42"),
      ItemStr("-4.42"),
      ItemStr("6.78"),
      ItemStr("6.78"),
      ItemStr("6.78"),
      ItemStr("6.78"),
      ItemStr("6.78"),
      ItemStr("6.78")
    )
    val valuesDbl: Seq[ItemDbl] = valuesStr.map {
      case ItemStr(x) => ItemDbl(Try(x.toDouble).toOption)
    }

    val isContainedValues: Check = Check(CheckLevel.Error, badColumnName)
      .isContainedIn(
        badColumnName,
        Array("NULL", "-10.0", "10.0", "0.0", "1.5245", "-4.42", "6.78"),
        _ >= 1.0,
        None
      )

    val isContainedBounds: Check = Check(CheckLevel.Error, badColumnName)
      .isContainedIn(
        badColumnName,
        -10.0,
        10.0,
        includeLowerBound = true,
        includeUpperBound = true,
        None
      )

    "generate correct Spark SQL & work for isContainedIn value list variant" in
      withSparkSession { sparkSession =>
        testWithExoticColumnName(
          sparkSession.createDataFrame(valuesStr),
          isContainedValues
        )
      }

    "generate correct Spark SQL & work for isContainedIn bounds variant" in
      withSparkSession { sparkSession =>
        testWithExoticColumnName(
          sparkSession.createDataFrame(valuesDbl),
          isContainedBounds
        )
      }
  }

  "Check isNewestPointNonAnomalous" should {

    "return the correct check status for anomaly detection for different analyzers" in
      withSparkSession { sparkSession =>
        evaluateWithRepository { repository =>
          // Fake Anomaly Detector
          val fakeAnomalyDetector = mock[AnomalyDetectionStrategy]
          inSequence {
            // Size results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(1.0, 2.0, 3.0, 4.0, 11.0), (4, 5))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(1.0, 2.0, 3.0, 4.0, 4.0), (4, 5))
              .returns(Seq((4, Anomaly(Option(4.0), 1.0))))
              .once()
            // Distinctness results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(1.0, 2.0, 3.0, 4.0, 1), (4, 5))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(1.0, 2.0, 3.0, 4.0, 1), (4, 5))
              .returns(Seq((4, Anomaly(Option(4.0), 0))))
              .once()
          }

          // Get test AnalyzerContexts
          val analysis = Analysis().addAnalyzers(Seq(Size(), Distinctness(Seq("c0", "c1"))))

          val context11Rows = analysis.run(getDfWithNRows(sparkSession, 11))
          val context4Rows = analysis.run(getDfWithNRows(sparkSession, 4))
          val contextNoRows = analysis.run(getDfEmpty(sparkSession))

          // Check isNewestPointNonAnomalous using Size
          val sizeAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector, Size(), Map.empty,
              None, None)

          assert(sizeAnomalyCheck.evaluate(context11Rows).status == CheckStatus.Success)
          assert(sizeAnomalyCheck.evaluate(context4Rows).status == CheckStatus.Error)
          assert(sizeAnomalyCheck.evaluate(contextNoRows).status == CheckStatus.Error)

          // Now with Distinctness
          val distinctnessAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector,
              Distinctness(Seq("c0", "c1")), Map.empty, None, None)

          assert(distinctnessAnomalyCheck.evaluate(context11Rows).status == CheckStatus.Success)
          assert(distinctnessAnomalyCheck.evaluate(context4Rows).status == CheckStatus.Error)
          assert(distinctnessAnomalyCheck.evaluate(contextNoRows).status == CheckStatus.Error)
        }
      }

     "only use historic results filtered by tagValues if specified" in
      withSparkSession { sparkSession =>
        evaluateWithRepository { repository =>
          // Fake Anomaly Detector
          val fakeAnomalyDetector = mock[AnomalyDetectionStrategy]
          inSequence {
            // Size results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(1.0, 2.0, 11.0), (2, 3))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(1.0, 2.0, 4.0), (2, 3))
              .returns(Seq((4, Anomaly(Option(4.0), 1.0))))
              .once()
          }

          // Get test AnalyzerContexts
          val analysis = Analysis().addAnalyzer(Size())

          val context11Rows = analysis.run(getDfWithNRows(sparkSession, 11))
          val context4Rows = analysis.run(getDfWithNRows(sparkSession, 4))
          val contextNoRows = analysis.run(getDfEmpty(sparkSession))

          // Check isNewestPointNonAnomalous using Size
          val sizeAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector, Size(),
              Map("Region" -> "EU"), None, None)

          assert(sizeAnomalyCheck.evaluate(context11Rows).status == CheckStatus.Success)
          assert(sizeAnomalyCheck.evaluate(context4Rows).status == CheckStatus.Error)
          assert(sizeAnomalyCheck.evaluate(contextNoRows).status == CheckStatus.Error)
        }
      }

    "only use historic results after some dateTime if specified" in
      withSparkSession { sparkSession =>
        evaluateWithRepository { repository =>
          // Fake Anomaly Detector
          val fakeAnomalyDetector = mock[AnomalyDetectionStrategy]
          inSequence {
            // Size results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(3.0, 4.0, 11.0), (2, 3))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(3.0, 4.0, 4.0), (2, 3))
              .returns(Seq((4, Anomaly(Option(4.0), 1.0))))
              .once()
          }

          // Get test AnalyzerContexts
          val analysis = Analysis().addAnalyzer(Size())

          val context11Rows = analysis.run(getDfWithNRows(sparkSession, 11))
          val context4Rows = analysis.run(getDfWithNRows(sparkSession, 4))
          val contextNoRows = analysis.run(getDfEmpty(sparkSession))

          // Check isNewestPointNonAnomalous using Size
          val sizeAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector, Size(),
              Map.empty, Some(3), None)

          assert(sizeAnomalyCheck.evaluate(context11Rows).status == CheckStatus.Success)
          assert(sizeAnomalyCheck.evaluate(context4Rows).status == CheckStatus.Error)
          assert(sizeAnomalyCheck.evaluate(contextNoRows).status == CheckStatus.Error)
        }
      }

    "only use historic results before some dateTime if specified" in
      withSparkSession { sparkSession =>
        evaluateWithRepository { repository =>
          // Fake Anomaly Detector
          val fakeAnomalyDetector = mock[AnomalyDetectionStrategy]
          inSequence {
            // Size results
            (fakeAnomalyDetector.detect _)
              .expects(Vector(1.0, 2.0, 11.0), (2, 3))
              .returns(Seq())
              .once()
            (fakeAnomalyDetector.detect _).expects(Vector(1.0, 2.0, 4.0), (2, 3))
              .returns(Seq((4, Anomaly(Option(4.0), 1.0))))
              .once()
          }

          // Get test AnalyzerContexts
          val analysis = Analysis().addAnalyzer(Size())

          val context11Rows = analysis.run(getDfWithNRows(sparkSession, 11))
          val context4Rows = analysis.run(getDfWithNRows(sparkSession, 4))
          val contextNoRows = analysis.run(getDfEmpty(sparkSession))

          // Check isNewestPointNonAnomalous using Size
          val sizeAnomalyCheck = Check(CheckLevel.Error, "anomaly test")
            .isNewestPointNonAnomalous(repository, fakeAnomalyDetector, Size(),
              Map.empty, None, Some(2))

          assert(sizeAnomalyCheck.evaluate(context11Rows).status == CheckStatus.Success)
          assert(sizeAnomalyCheck.evaluate(context4Rows).status == CheckStatus.Error)
          assert(sizeAnomalyCheck.evaluate(contextNoRows).status == CheckStatus.Error)
        }
      }
  }

  /** Run anomaly detection using a repository with some previous analysis results for testing */
  private[this] def evaluateWithRepository(test: MetricsRepository => Unit): Unit = {

    val repository = createRepository()

    (1 to 2).foreach { timeStamp =>
      val analyzerContext = new AnalyzerContext(Map(
        Size() -> DoubleMetric(Entity.Column, "", "", Success(timeStamp)),
        Distinctness(Seq("c0", "c1")) -> DoubleMetric(Entity.Column, "", "",
          Success(timeStamp))
      ))
      repository.save(ResultKey(timeStamp, Map("Region" -> "EU")), analyzerContext)
    }

    (3 to 4).foreach { timeStamp =>
      val analyzerContext = new AnalyzerContext(Map(
        Size() -> DoubleMetric(Entity.Column, "", "", Success(timeStamp)),
        Distinctness(Seq("c0", "c1")) -> DoubleMetric(Entity.Column, "", "",
          Success(timeStamp))
      ))
      repository.save(ResultKey(timeStamp, Map("Region" -> "NA")), analyzerContext)
    }
    test(repository)
  }

   /** Create a repository for testing */
  private[this] def createRepository(): MetricsRepository = {
    new InMemoryMetricsRepository()
  }
}

object CheckTest extends WordSpec with Matchers {

  def assertSuccess(check: Check, context: AnalyzerContext): Unit = {
    check.evaluate(context).status shouldBe CheckStatus.Success
  }

  def assertEvaluatesTo(
    check: Check,
    context: AnalyzerContext,
    status: CheckStatus.Value)
  : Unit = {

    assert(check.evaluate(context).status == status)
  }

  def runChecks(data: DataFrame, check: Check, checks: Check*): AnalyzerContext = {
    val analyzers = (check.requiredAnalyzers() ++ checks.flatMap { _.requiredAnalyzers() }).toSeq

    Analysis(analyzers).run(data)
  }

  private[this] def runAndAssertSuccessFor[T](
    checkOn: String => Check, dataType: NumericType, sparkSession: SparkSession
  ): Unit = {
    val col = "some"
    val numericRow = dataType match {
      case FloatType => Row(1.0f)
      case DoubleType => Row(1.0d)
      case ByteType => Row(1.toByte)
      case ShortType => Row(1.toShort)
      case IntegerType => Row(1)
      case LongType => Row(1L)
    }
    val df = dataFrameWithColumn(col, dataType, sparkSession, numericRow, Row(null))
    val check = checkOn(col)
    val context = runChecks(df, check)
    assertEvaluatesTo(check, context, CheckStatus.Success)
  }

  def assertNonNegativeCheckIsSuccessFor(
    dataType: NumericType,
    sparkSession: SparkSession)
  : Unit = {

    runAndAssertSuccessFor(Check(CheckLevel.Error, "some description").isNonNegative(_),
      dataType, sparkSession)
  }

  def assertIsPositiveCheckIsSuccessFor(
    dataType: NumericType,
    sparkSession: SparkSession)
  : Unit = {

    runAndAssertSuccessFor(Check(CheckLevel.Error, "some description").isPositive(_),
      dataType, sparkSession)
  }

  val badColumnName: String = "[this column]:has a handful of problematic chars"

  case class ItemStr(`[this column]:has a handful of problematic chars`: String)
  case class ItemDbl(`[this column]:has a handful of problematic chars`: Option[Double])
}
