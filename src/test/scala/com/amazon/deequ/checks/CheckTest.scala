/**
  * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.anomalydetection.Anomaly
import com.amazon.deequ.anomalydetection.AnomalyDetectionStrategy
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.repository.MetricsRepository
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Success
import scala.util.Try

class CheckTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport
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

      val check4 = Check(CheckLevel.Error, "group-3")
        .isComplete("att2", None) // 1.0 with filter
        .where("att2 is NOT NULL")
        .hasCompleteness("att2", _ == 1.0, None) // 1.0 with filter
        .where("att2 is NOT NULL")

      val context = runChecks(getDfCompleteAndInCompleteColumns(sparkSession),
        check1, check2, check3, check4)

      context.metricMap.foreach { println }

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Error)
      assertEvaluatesTo(check3, context, CheckStatus.Warning)
      assertEvaluatesTo(check4, context, CheckStatus.Success)

      assert(check1.getRowLevelConstraintColumnNames() == Seq("Completeness-att1", "Completeness-att1"))
      assert(check2.getRowLevelConstraintColumnNames() == Seq("Completeness-att2"))
      assert(check3.getRowLevelConstraintColumnNames() == Seq("Completeness-att2"))
      assert(check4.getRowLevelConstraintColumnNames() == Seq("Completeness-att2", "Completeness-att2"))
    }

    "return the correct check status for completeness with where filter" in withSparkSession { sparkSession =>

      val check = Check(CheckLevel.Error, "group-3")
        .hasCompleteness("ZipCode", _ > 0.6, None) // 1.0 with filter
        .where("City is NOT NULL")

      val context = runChecks(getDfForWhereClause(sparkSession), check)

      assertEvaluatesTo(check, context, CheckStatus.Success)

      assert(check.getRowLevelConstraintColumnNames() == Seq("Completeness-ZipCode"))
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

    "return the correct check status for combined completeness with . in column name" in
      withSparkSession { sparkSession =>

        val check1 = Check(CheckLevel.Error, "group-1")
          .areComplete(Seq("`item.one`", "`att.1`")) // 1.0
          .haveCompleteness(Seq("`item.one`", "`att.1`"), _ == 1.0) // 1.0

        val check2 = Check(CheckLevel.Error, "group-2-E")
          .haveCompleteness(Seq("`item.one`", "`att.1`", "`att.2`"), _ > 0.8) // 0.75

        val check3 = Check(CheckLevel.Warning, "group-2-W")
          .haveCompleteness(Seq("`item.one`", "`att.1`", "`att.2`"), _ > 0.8) // 0.75

        val context = runChecks(getDfCompleteAndInCompleteColumnsWithPeriod(sparkSession),
          check1, check2, check3)

        context.metricMap.foreach {
          println
        }

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
        .areUnique(Seq("nonUnique", "onlyUniqueWithOtherNonUnique"))
        .areUnique(Seq("nonUnique", "halfUniqueCombinedWithNonUnique"))

      val context = runChecks(getDfWithUniqueColumns(sparkSession), check)
      val result = check.evaluate(context)

      assert(result.status == CheckStatus.Error)
      val constraintStatuses = result.constraintResults.map(_.status)
      assert(constraintStatuses.head == ConstraintStatus.Success)
      assert(constraintStatuses(1) == ConstraintStatus.Success)
      assert(constraintStatuses(2) == ConstraintStatus.Success)
      assert(constraintStatuses(3) == ConstraintStatus.Failure)
      assert(constraintStatuses(4) == ConstraintStatus.Failure)
      assert(constraintStatuses(5) == ConstraintStatus.Success)
      assert(constraintStatuses(6) == ConstraintStatus.Failure)
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
        .satisfies("att1 > 0", "rule1", columns = List("att1"))

      val check2 = Check(CheckLevel.Error, "group-2-to-fail")
        .satisfies("att1 > 3", "rule2", columns = List("att1"))

      val check3 = Check(CheckLevel.Error, "group-2-to-succeed")
        .satisfies("att1 > 3", "rule3", _ == 0.5, columns = List("att1"))

      val context = runChecks(getDfWithNumericValues(sparkSession), check1, check2, check3)

      assertEvaluatesTo(check1, context, CheckStatus.Success)
      assertEvaluatesTo(check2, context, CheckStatus.Error)
      assertEvaluatesTo(check3, context, CheckStatus.Success)
    }

    "return the correct check status for conditional column constraints" in
      withSparkSession { sparkSession =>

        val checkToSucceed = Check(CheckLevel.Error, "group-1")
          .satisfies("att1 < att2", "rule1", columns = List("att1")).where("att1 > 3")

        val checkToFail = Check(CheckLevel.Error, "group-1")
          .satisfies("att2 > 0", "rule2", columns = List("att1")).where("att1 > 0")

        val checkPartiallyGetsSatisfied = Check(CheckLevel.Error, "group-1")
          .satisfies("att2 > 0", "rule3", _ == 0.5, columns = List("att1")).where("att1 > 0")

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

    "correctly evaluate range constraints when values have single quote in string" in withSparkSession { sparkSession =>
      val rangeCheck = Check(CheckLevel.Error, "a")
        .isContainedIn("att2", Array("can't", "help", "but", "wouldn't"))

      val rangeResults = runChecks(getDfWithDistinctValuesQuotes(sparkSession), rangeCheck)
      assertEvaluatesTo(rangeCheck, rangeResults, CheckStatus.Success)
    }

    "return the correct check status for histogram constraints" in
      withSparkSession { sparkSession =>

        // Basic categorical value assertions
        val check1 = Check(CheckLevel.Error, "basic-categorical-tests")
          .hasNumberOfDistinctValues("att1", _ < 10)
          .hasHistogramValues("att1", _ ("a").absolute == 4)  // Value "a" appears exactly 4 times
          .hasHistogramValues("att1", _ ("b").absolute == 2)  // Value "b" appears exactly 2 times
          .hasHistogramValues("att1", _ ("a").ratio > 0.6)    // Value "a" is >60% of data
          .hasHistogramValues("att1", _ ("b").ratio < 0.4)    // Value "b" is <40% of data

        // Filtered constraint tests (with WHERE clauses)
        val check2 = Check(CheckLevel.Error, "filtered-constraint-tests")
          .hasHistogramValues("att1", _ ("a").absolute == 3)
          .where("att2 is not null") // Filtered: "a" appears 3 times when att2 not null
          .hasHistogramValues("att1", _ ("b").absolute == 1)
          .where("att2 is not null") // Filtered: "b" appears 1 time when att2 not null

        // Null value handling tests
        val check3 = Check(CheckLevel.Error, "null-handling-tests")
          .hasNumberOfDistinctValues("att2", _ == 3)
          .hasNumberOfDistinctValues("att2", _ == 2).where("att1 = 'a'")
          .hasHistogramValues("att2", _ ("f").absolute == 3) // Value "f" appears 3 times
          .hasHistogramValues("att2", _ ("d").absolute == 1) // Value "d" appears 1 time
          .hasHistogramValues("att2", _ (Histogram.NullFieldReplacement).absolute == 2) // Nulls appear 2 times
          .hasHistogramValues("att2", _ ("f").ratio == 3 / 6.0) // Value "f" is exactly 50%
          .hasHistogramValues("att2", _ ("d").ratio == 1 / 6.0) // Value "d" is exactly 16.67%
          .hasHistogramValues("att2", _ (Histogram.NullFieldReplacement).ratio == 2 / 6.0)

        // Edge case tests (boundary conditions, empty categories)
        val check4 = Check(CheckLevel.Error, "edge-case-tests")
          .hasHistogramValues("att1", !_.values.contains("nonexistent"))  // Fake category not present
          .hasHistogramValues("att2", _ ("f").ratio <= 1.0)               // Ratio boundary: <= 1.0
          .hasHistogramValues("att2", _ ("d").ratio >= 0.0)               // Ratio boundary: >= 0.0

        // Complex filter conditions (multiple WHERE clauses)
        val check5 = Check(CheckLevel.Error, "complex-filter-tests")
          .hasHistogramValues("att1", _ ("a").absolute >= 1)
          .where("att2 = 'f'")          // Complex filter: att1="a" when att2="f"
          .hasHistogramValues("att2", _ ("f").absolute >= 2)
          .where("att1 in ('a', 'b')")  // Complex filter: att2="f" when att1 in set

        // maxBins parameter tests (different bin limits show different distinct values)
        val check6 = Check(CheckLevel.Error, "maxBins-parameter-tests")
          .hasHistogramValues("att1", _ ("a").absolute == 4, maxBins = 10)  // Custom maxBins = 10
          .hasHistogramValues("att2", _ ("f").absolute == 3, maxBins = 5)   // Custom maxBins = 5
          .hasHistogramValues("att2",  // With maxBins=1, only top value "f" tracked, "d" not present
            !_.values.contains("d"), maxBins = 1)

        // aggregate function tests
        val numericDf = sparkSession.createDataFrame(Seq(
          ("a", 10), ("a", 20), ("b", 30), ("b", 40)
        )).toDF("category", "value")

        val check7 = Check(CheckLevel.Error, "aggregate-function-tests")
          .hasHistogramValues("category", _ ("a").absolute == 2) // Count aggregation (default)

        // failure cases (error conditions)
        val check8 = Check(CheckLevel.Error, "failure-tests")
          .hasNumberOfDistinctValues("unKnownColumn", _ == 3) // Fake column should fail

        val context1 = runChecks(getDfCompleteAndInCompleteColumns(sparkSession),
          check1, check2, check3, check4, check5, check6, check8)
        val context2 = runChecks(numericDf, check7)

        assertEvaluatesTo(check1, context1, CheckStatus.Success)
        assertEvaluatesTo(check2, context1, CheckStatus.Success)
        assertEvaluatesTo(check3, context1, CheckStatus.Success)
        assertEvaluatesTo(check4, context1, CheckStatus.Success)
        assertEvaluatesTo(check5, context1, CheckStatus.Success)
        assertEvaluatesTo(check6, context1, CheckStatus.Success)
        assertEvaluatesTo(check7, context2, CheckStatus.Success)
        assertEvaluatesTo(check8, context1, CheckStatus.Error)
      }

    "return the correct check status for histogram binned constraints" in
      withSparkSession { sparkSession =>

        // Create test data with known distribution (20 values, 5 bins)
        val df = sparkSession.createDataFrame(Seq(
          (1, Some(10.0)), (2, Some(12.0)), (3, Some(15.0)), (4, Some(18.0)), (5, Some(20.0)),
          (6, Some(25.0)), (7, Some(28.0)), (8, Some(30.0)), (9, Some(32.0)), (10, Some(35.0)),
          (11, Some(40.0)), (12, Some(42.0)), (13, Some(45.0)), (14, Some(48.0)), (15, Some(50.0)),
          (16, Some(55.0)), (17, Some(58.0)), (18, Some(60.0)), (19, Some(65.0)), (20, None)
        )).toDF("id", "value")

        // Bin-specific assertions
        val check1 = Check(CheckLevel.Error, "bin-specific-tests")
          .hasHistogramBinnedValues("value", _.bins(0).frequency >= 1, Some(5))   // First bin has >=1 value
          .hasHistogramBinnedValues("value", _.bins(2).ratio >= 0.0, Some(5))     // Third bin has some ratio
          .hasHistogramBinnedValues("value", _.bins.last.frequency >= 0, Some(5)) // Last bin exists

        // Null handling
        val check2 = Check(CheckLevel.Error, "null-handling-tests")
          // Null bin exists and has one value
          .hasHistogramBinnedValues("value", _.bins.exists(_.binStart == Double.NegativeInfinity), Some(5))
          .hasHistogramBinnedValues("value",
            _.bins.filter(_.binStart == Double.NegativeInfinity).head.frequency == 1, Some(5))

        // Distribution shape tests
        val check3 = Check(CheckLevel.Error, "distribution-shape-tests")
          .hasHistogramBinnedValues("value",
            _.bins.count(_.frequency > 0) >= 3, Some(5))  // At least 3 non-empty bins
          .hasHistogramBinnedValues("value",
            _.bins.exists(_.frequency > 2), Some(5))      // Some bin has >2 values (peak detection)
          .hasHistogramBinnedValues("value",
            _.bins.forall(_.frequency <= 20), Some(5))    // No bin is too large (outlier detection)

        // Range / interval tests
        val check4 = Check(CheckLevel.Error, "range-interval-tests")
          .hasHistogramBinnedValues("value", _.bins.filter(b => b.binStart >= 20 && b.binEnd <= 40)
            .map(_.frequency).sum >= 2, Some(5))      // Values in 20-40 range
          .hasHistogramBinnedValues("value",  // First bin starts at reasonable value
            _.bins(0).binStart <= 15, Some(5))
          .hasHistogramBinnedValues("value",  // Last bin ends at reasonable value
            _.bins.last.binEnd >= 60, Some(5))

        // Statistical distribution tests
        val check5 = Check(CheckLevel.Error, "statistical-tests")
          .hasHistogramBinnedValues("value",
            _.bins.maxBy(_.frequency).binStart >= 0, Some(5))  // Peak is in reasonable range
          .hasHistogramBinnedValues("value",
            _.bins.take(2).map(_.frequency).sum >= 1, Some(5)) // Left bins have some data
          .hasHistogramBinnedValues("value",
            _.bins.forall(b => b.frequency >= 0), Some(5))     // All frequencies non-negative

        // Bin structure tests
        val check6 = Check(CheckLevel.Error, "bin-structure-tests")
          .hasHistogramBinnedBins("value", _ >= 5, Some(5))                 // Expected number of bins
          .hasHistogramBinnedValues("value", _.numberOfBins >= 5, Some(5))  // numberOfBins matches
          .hasHistogramBinnedValues("value", _.bins.filter(_.binStart != Double.NegativeInfinity)
            .forall(b => b.binEnd > b.binStart), Some(5)) // Valid bin ranges

        // Filtered constraint tests (WHERE clauses)
        val check7 = Check(CheckLevel.Error, "filtered-binned-tests")
          .hasHistogramBinnedValues("value", _.bins.exists(_.frequency > 0), Some(5))
          .where("id <= 10")    // Filter to first 10 rows
          .hasHistogramBinnedBins("value", _ >= 3, Some(5))
          .where("value > 20")  // Filter to values > 20

        // Aggregate function tests
        val numericDf = sparkSession.createDataFrame(Seq(
          (1, 10.0, 5), (2, 15.0, 3), (3, 25.0, 7), (4, 35.0, 2), (5, 45.0, 8)
        )).toDF("id", "value", "weight")

        val check8 = Check(CheckLevel.Error, "aggregate-binned-tests")
          .hasHistogramBinnedValues("value", _.bins.exists(_.frequency > 0), Some(3))
          .hasHistogramBinnedValues("value", _.bins.forall(_.frequency >= 0), Some(3))

        // Failure cases
        val check9 = Check(CheckLevel.Error, "failure-tests")
          .hasHistogramBinnedValues("value", _.bins.isEmpty, Some(5)) // Should fail - no bins
          .hasHistogramBinnedBins("value", _ == 0, Some(5))           // Should fail - zero bins

        val context1 = runChecks(df, check1, check2, check3, check4, check5, check6, check9)
        val context2 = runChecks(df, check7)  // Filtered tests on original data
        val context3 = runChecks(numericDf, check8)  // Aggregate tests on numeric data

        assertEvaluatesTo(check1, context1, CheckStatus.Success)
        assertEvaluatesTo(check2, context1, CheckStatus.Success)
        assertEvaluatesTo(check3, context1, CheckStatus.Success)
        assertEvaluatesTo(check4, context1, CheckStatus.Success)
        assertEvaluatesTo(check5, context1, CheckStatus.Success)
        assertEvaluatesTo(check6, context1, CheckStatus.Success)
        assertEvaluatesTo(check7, context2, CheckStatus.Success)
        assertEvaluatesTo(check8, context3, CheckStatus.Success)
        assertEvaluatesTo(check9, context1, CheckStatus.Error)
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

      val numericAnalysis = AnalysisRunner.onData(dfNumeric).addAnalyzers(Seq(
        Minimum("att1"), Maximum("att1"), Mean("att1"), Sum("att1"),
        StandardDeviation("att1"), ApproxCountDistinct("att1"),
        ApproxQuantile("att1", quantile = 0.5), ExactQuantile("att1", quantile = 0.5)))

      val contextNumeric = numericAnalysis.run()

      assertSuccess(baseCheck.hasMin("att1", _ == 1.0), contextNumeric)
      assertSuccess(baseCheck.hasMax("att1", _ == 6.0), contextNumeric)
      assertSuccess(baseCheck.hasMean("att1", _ == 3.5), contextNumeric)
      assertSuccess(baseCheck.hasSum("att1", _ == 21.0), contextNumeric)
      assertSuccess(baseCheck.hasStandardDeviation("att1", _ == 1.707825127659933), contextNumeric)
      assertSuccess(baseCheck.hasApproxCountDistinct("att1", _ == 6.0), contextNumeric)
      assertSuccess(baseCheck.hasApproxQuantile("att1", quantile = 0.5, _ == 3.0), contextNumeric)
      assertSuccess(baseCheck.hasExactQuantile("att1", quantile = 0.5, _ == 3.5), contextNumeric)

      val correlationAnalysisInformative = AnalysisRunner.onData(dfInformative)
        .addAnalyzer(Correlation("att1", "att2"))
      val correlationAnalysisUninformative = AnalysisRunner.onData(dfUninformative)
        .addAnalyzer(Correlation("att1", "att2"))

      val contextInformative = correlationAnalysisInformative.run()
      val contextUninformative = correlationAnalysisUninformative.run()

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

    "correctly evaluate hasExactQuantile constraints" in withSparkSession { sparkSession =>
      val hasExactQuantileCheck = Check(CheckLevel.Error, "a")
        .hasExactQuantile("att1", quantile = 0.5, _ == 3.5)
      val hasExactQuantileCheckWithFilter = Check(CheckLevel.Error, "a")
        .hasExactQuantile("att1", quantile = 0.5, _ == 5.0).where("att2 > 0")

      val context = runChecks(getDfWithNumericValues(sparkSession), hasExactQuantileCheck,
        hasExactQuantileCheckWithFilter)

      assertSuccess(hasExactQuantileCheck, context)
      assertSuccess(hasExactQuantileCheckWithFilter, context)
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

    "yield correct results for minimum and maximum length stats with where clause" in
      withSparkSession { sparkSession =>
        val emptyNulLBehavior = Option(AnalyzerOptions(NullBehavior.EmptyString))
        val baseCheck = Check(CheckLevel.Error, description = "a description")
        val df = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(sparkSession)
        val context = AnalysisRunner.onData(df)
          .addAnalyzers(Seq(MinLength("item", Option("val1 > 3"), emptyNulLBehavior),
            MaxLength("item", Option("val1 <= 3"), emptyNulLBehavior))).run()

        assertSuccess(baseCheck.hasMinLength("item", _ >= 4.0, analyzerOptions = emptyNulLBehavior)
          .where("val1 > 3"), context) // 1 without where clause
        assertSuccess(baseCheck.hasMaxLength("item", _ <= 3.0, analyzerOptions = emptyNulLBehavior)
          .where("val1 <= 3"), context) // 6 without where clause
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

    "handle fractional values in scientific notations" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val df = Seq(
        ("1.0"),
        ("1.0000"),
        ("1.0001"),
        ("1.0E-3"),
        ("1.0e-3"),
        ("1E-3")
      ).toDF("val")

      val datatypeCheck = Check(CheckLevel.Error, "they're all fractional")
        .hasDataType("val", ConstrainableDataTypes.Fractional, _ == 1.0)
      val datatypeContext = runChecks(df, datatypeCheck)
      assertEvaluatesTo(datatypeCheck, datatypeContext, CheckStatus.Success)

      val nonNegativeCheck = Check(CheckLevel.Error, "they're positive")
        .isNonNegative("val")
      val nonNegativeContext = runChecks(df, nonNegativeCheck)
      assertEvaluatesTo(nonNegativeCheck, nonNegativeContext, CheckStatus.Success)
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

          val context11Rows = AnalysisRunner.run(getDfWithNRows(sparkSession, 11), analysis)
          val context4Rows = AnalysisRunner.run(getDfWithNRows(sparkSession, 4), analysis)
          val contextNoRows = AnalysisRunner.run(getDfEmpty(sparkSession), analysis)

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

          val context11Rows = AnalysisRunner.run(getDfWithNRows(sparkSession, 11), analysis)
          val context4Rows = AnalysisRunner.run(getDfWithNRows(sparkSession, 4), analysis)
          val contextNoRows = AnalysisRunner.run(getDfEmpty(sparkSession), analysis)

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

          val context11Rows = AnalysisRunner.run(getDfWithNRows(sparkSession, 11), analysis)
          val context4Rows = AnalysisRunner.run(getDfWithNRows(sparkSession, 4), analysis)
          val contextNoRows = AnalysisRunner.run(getDfEmpty(sparkSession), analysis)

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

          val context11Rows = AnalysisRunner.run(getDfWithNRows(sparkSession, 11), analysis)
          val context4Rows = AnalysisRunner.run(getDfWithNRows(sparkSession, 4), analysis)
          val contextNoRows = AnalysisRunner.run(getDfEmpty(sparkSession), analysis)

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

  /**
   * Test for DataSync in verification suite.
   */
  "Check hasDataInSync" should {

    val colMapAtt1 = Map("att1" -> "att1")
    val colMapTwoCols = Map("att1" -> "att1", "att2" -> "att2")

    "yield success for basic data sync test for 1 col" in withSparkSession { sparkSession =>
      val dfInformative = getDfWithConditionallyInformativeColumns(sparkSession)

      val check = Check(CheckLevel.Error, "must have data in sync")
        .doesDatasetMatch(dfInformative, colMapAtt1, _ > 0.9, hint = Some("show be in sync"))
      val context = runChecks(dfInformative, check)

      assertSuccess(check, context)

      val check2 = Check(CheckLevel.Error, "must have data in sync")
        .doesDatasetMatch(dfInformative, colMapAtt1, _ > 0.9, Some(colMapAtt1), Some("show be in sync with match col"))
      val context2 = runChecks(dfInformative, check2)

      assertSuccess(check2, context2)
    }

    "yield failure when column doesnt exist in data sync test for 1 col" in withSparkSession { sparkSession =>
      val dfInformative = getDfWithConditionallyInformativeColumns(sparkSession)
      val dfInformativeRenamed = dfInformative.withColumnRenamed("att1", "att1_renamed")

      val check = Check(CheckLevel.Error, "must fail as columns does not exist")
        .doesDatasetMatch(dfInformativeRenamed, colMapAtt1, _ > 0.9,
          hint = Some("must fail as columns does not exist"))
      val context = runChecks(dfInformative, check)
      assertEvaluatesTo(check, context, CheckStatus.Error)

    }

    "yield failure when row count varies in data sync test for 1 col" in withSparkSession { sparkSession =>
      val dfInformative = getDfWithConditionallyInformativeColumns(sparkSession)
      val dfInformativeFiltered = dfInformative.filter("att1 > 2")

      val check = Check(CheckLevel.Error, "must fail as columns does not exist")
        .doesDatasetMatch(dfInformativeFiltered, colMapAtt1, _ > 0.9,
          hint = Some("must fail as columns does not exist"))
      val context = runChecks(dfInformative, check)
      assertEvaluatesTo(check, context, CheckStatus.Error)
    }

    "yield failed assertion for 0.9 for 1 col" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyInformativeColumns(sparkSession)
      val modifiedDf = df.withColumn("att1", when(col("att1") === 3, 4)
        .otherwise(col("att1")))

      val check = Check(CheckLevel.Error, "must fail as rows mismatches")
        .doesDatasetMatch(modifiedDf, colMapAtt1, _ > 0.9, hint = Some("must fail as rows mismatches"))
      val context = runChecks(df, check)
      assertEvaluatesTo(check, context, CheckStatus.Error)

    }

    "yield failed assertion for 0.6 for 1 col" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyInformativeColumns(sparkSession)
      val modifiedDf = df.withColumn("att1", when(col("att1") === 3, 4)
        .otherwise(col("att1")))

      val check = Check(CheckLevel.Error, "must be success as rows count mismatches at assertion 0.6")
        .doesDatasetMatch(modifiedDf, colMapAtt1, _ > 0.6,
          hint = Some("must be success as rows count mismatches at assertion 0.6"))
      val context = runChecks(df, check)
      assertSuccess(check, context)
    }


    "yield success for basic data sync test for multiple columns" in withSparkSession { sparkSession =>
      val dfInformative = getDfWithConditionallyInformativeColumns(sparkSession)

      val check = Check(CheckLevel.Error, "must have data in sync")
        .doesDatasetMatch(dfInformative, colMapTwoCols, _ > 0.9, hint = Some("show be in sync"))
      val context = runChecks(dfInformative, check)

      assertSuccess(check, context)
    }

    "yield success for basic data sync test for multiple columns and one col match" in
      withSparkSession { sparkSession =>
        val dfInformative = getDfWithConditionallyInformativeColumns(sparkSession)

        val check = Check(CheckLevel.Error, "must have data in sync")
          .doesDatasetMatch(dfInformative, colMapTwoCols, _ > 0.9, Some(colMapAtt1), hint = Some("show be in sync"))
        val context = runChecks(dfInformative, check)

        assertSuccess(check, context)
      }

    "yield failure when column doesnt exist in data sync test for multiple columns" in withSparkSession {
      sparkSession =>
        val dfInformative = getDfWithConditionallyInformativeColumns(sparkSession)
        val dfInformativeRenamed = dfInformative.withColumnRenamed("att1", "att1_renamed")

        val check = Check(CheckLevel.Error, "must fail as columns does not exist")
          .doesDatasetMatch(dfInformativeRenamed, colMapTwoCols, _ > 0.9,
            hint = Some("must fail as columns does not exist"))
        val context = runChecks(dfInformative, check)

        assertEvaluatesTo(check, context, CheckStatus.Error)
    }

    "yield failure when row count varies in data sync test for multiple columns" in withSparkSession { sparkSession =>
      val dfInformative = getDfWithConditionallyInformativeColumns(sparkSession)
      val dfInformativeFiltered = dfInformative.filter("att1 > 2")

      val check = Check(CheckLevel.Error, "must fail as columns does not exist")
        .doesDatasetMatch(dfInformativeFiltered, colMapTwoCols, _ > 0.9,
          hint = Some("must fail as columns does not exist"))
      val context = runChecks(dfInformative, check)

      assertEvaluatesTo(check, context, CheckStatus.Error)
    }

    "yield failed assertion for 0.9 for multiple columns" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyInformativeColumns(sparkSession)
      val modifiedDf = df.withColumn("att1", when(col("att1") === 3, 4)
        .otherwise(col("att1")))

      val check = Check(CheckLevel.Error, "must fail as rows mismatches")
        .doesDatasetMatch(modifiedDf, colMapTwoCols, _ > 0.9, hint = Some("must fail as rows mismatches"))
      val context = runChecks(df, check)

      assertEvaluatesTo(check, context, CheckStatus.Error)

    }

    "yield failed assertion for 0.6 for multiple columns" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyInformativeColumns(sparkSession)
      val modifiedDf = df.withColumn("att1", when(col("att1") === 3, 4)
        .otherwise(col("att1")))

      val check = Check(CheckLevel.Error, "must be success as metric value is 0.66")
        .doesDatasetMatch(modifiedDf, colMapTwoCols, _ > 0.6,
          hint = Some("must be success as metric value is 0.66"))
      val context = runChecks(df, check)

      assertSuccess(check, context)
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

object CheckTest extends AnyWordSpec with Matchers {

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

    AnalysisRunner.run(data, Analysis(analyzers))
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
