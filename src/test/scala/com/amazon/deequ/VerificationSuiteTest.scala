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

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.anomalydetection.AbsoluteChangeStrategy
import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.Constraint
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.io.DfsUtils
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.MetricsRepository
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.utils.CollectionUtils.SeqExtensions
import com.amazon.deequ.utils.FixtureSupport
import com.amazon.deequ.utils.TempFileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.scalatest.WordSpec

class VerificationSuiteTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport with MockFactory {

  "Verification Suite" should {
    "return the correct verification status regardless of the order of checks" in
      withSparkSession { sparkSession =>

        def assertStatusFor(data: DataFrame, checks: Check*)
                           (expectedStatus: CheckStatus.Value)
          : Unit = {
          val verificationSuiteStatus =
            VerificationSuite().onData(data).addChecks(checks).run().status
          assert(verificationSuiteStatus == expectedStatus)
        }

        val df = getDfCompleteAndInCompleteColumns(sparkSession)

        val checkToSucceed = Check(CheckLevel.Error, "group-1")
          .isComplete("att1")
          .hasColumnCount(_ == 3)
          .hasCompleteness("att1", _ == 1.0)

        val checkToErrorOut = Check(CheckLevel.Error, "group-2-E")
          .hasCompleteness("att2", _ > 0.8)

        val checkToWarn = Check(CheckLevel.Warning, "group-2-W")
          .hasCompleteness("item", _ < 0.8)


        assertStatusFor(df, checkToSucceed)(CheckStatus.Success)
        assertStatusFor(df, checkToErrorOut)(CheckStatus.Error)
        assertStatusFor(df, checkToWarn)(CheckStatus.Warning)


        Seq(checkToSucceed, checkToErrorOut).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Error)
        }

        Seq(checkToSucceed, checkToWarn).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Warning)
        }

        Seq(checkToWarn, checkToErrorOut).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Error)
        }

        Seq(checkToSucceed, checkToWarn, checkToErrorOut).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Error)
        }
      }

    "return the correct verification status regardless of the order of checks with period" in
      withSparkSession { sparkSession =>

        def assertStatusFor(data: DataFrame, checks: Check*)
                           (expectedStatus: CheckStatus.Value)
        : Unit = {
          val verificationSuiteStatus =
            VerificationSuite().onData(data).addChecks(checks).run().status
          assert(verificationSuiteStatus == expectedStatus)
        }

        val df = getDfCompleteAndInCompleteColumnsWithPeriod(sparkSession)

        val checkToSucceed = Check(CheckLevel.Error, "group-1")
          .isComplete("`att.1`")
          .hasCompleteness("`att.1`", _ == 1.0)

        val checkToErrorOut = Check(CheckLevel.Error, "group-2-E")
          .hasCompleteness("`att.2`", _ > 0.8)

        val checkToWarn = Check(CheckLevel.Warning, "group-2-W")
          .hasCompleteness("`item.one`", _ < 0.8)


        assertStatusFor(df, checkToSucceed)(CheckStatus.Success)
        assertStatusFor(df, checkToErrorOut)(CheckStatus.Error)
        assertStatusFor(df, checkToWarn)(CheckStatus.Warning)


        Seq(checkToSucceed, checkToErrorOut).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Error)
        }

        Seq(checkToSucceed, checkToWarn).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Warning)
        }

        Seq(checkToWarn, checkToErrorOut).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Error)
        }

        Seq(checkToSucceed, checkToWarn, checkToErrorOut).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Error)
        }
      }

    "generate a result that aggregates all constraint results" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumns(session)

      val isComplete = new Check(CheckLevel.Error, "rule1")
        .isComplete("att1")
        .isComplete("att2")
      val expectedColumn1 = isComplete.description

      val suite = new VerificationSuite().onData(data).addChecks(Seq(isComplete))

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)
      resultData.show()

      val expectedColumns: Seq[String] = data.columns :+ expectedColumn1
      assert(resultData.columns.sameElements(expectedColumns))

      val rowLevel = resultData.select(expectedColumn1).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel))

    }

    "test uniqueness" in withSparkSession { session =>
      val data = getDfWithUniqueColumns(session)

      val overlapUniqueness = new Check(CheckLevel.Error, "rule1")
        .hasUniqueness(Seq("nonUnique", "halfUniqueCombinedWithNonUnique"), Check.IsOne)
      val hasFullUniqueness = new Check(CheckLevel.Error, "rule2")
        .hasUniqueness(Seq("nonUnique", "onlyUniqueWithOtherNonUnique"), Check.IsOne)
      val uniquenessWithNulls = new Check(CheckLevel.Error, "rule3")
        .hasUniqueness(Seq("unique", "nonUniqueWithNulls"), Check.IsOne)
      val unique = new Check(CheckLevel.Error, "rule4").isUnique("unique")
      val nonUnique = new Check(CheckLevel.Error, "rule5").isUnique("nonUnique")
      val nullPrimaryKey = new Check(CheckLevel.Error, "rule6").isPrimaryKey("uniqueWithNulls")
      val uniqueValueRatio = new Check(CheckLevel.Error, "rule7")
        .hasUniqueValueRatio(Seq("nonUnique"), _ == 0.75)

      val expectedColumn1 = overlapUniqueness.description
      val expectedColumn2 = hasFullUniqueness.description
      val expectedColumn3 = uniquenessWithNulls.description
      val expectedColumn4 = unique.description
      val expectedColumn5 = nonUnique.description
      val expectedColumn6 = nullPrimaryKey.description
      val expectedColumn7 = uniqueValueRatio.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(overlapUniqueness)
        .addCheck(hasFullUniqueness)
        .addCheck(uniquenessWithNulls)
        .addCheck(unique)
        .addCheck(nonUnique)
        .addCheck(nullPrimaryKey)
        .addCheck(uniqueValueRatio)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)
      resultData.show()

      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 + expectedColumn4 +
          expectedColumn5 + expectedColumn6 + expectedColumn7
      assert(resultData.columns.toSet == expectedColumns)

      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.get(0))
      assert(Seq(false, false, false, true, true, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.get(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel2))

      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.get(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel3))

      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.get(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel4))

      val rowLevel5 = resultData.select(expectedColumn5).collect().map(r => r.get(0))
      assert(Seq(false, false, false, true, true, true).sameElements(rowLevel5))

      // TODO: fix how primaryKey works (nulls should be false)
      val rowLevel6 = resultData.select(expectedColumn6).collect().map(r => r.get(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel6))

      val rowLevel7 = resultData.select(expectedColumn7).collect().map(r => r.get(0))
      assert(Seq(false, false, false, true, true, true).sameElements(rowLevel7))
    }

    "generate a result that contains row-level results" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(session)

      val isComplete = new Check(CheckLevel.Error, "rule1").isComplete("att1")
      val completeness = new Check(CheckLevel.Error, "rule2").hasCompleteness("att2", _ > 0.7)
      val isPrimaryKey = new Check(CheckLevel.Error, "rule3").isPrimaryKey("item")
      val minLength = new Check(CheckLevel.Error, "rule4")
        .hasMinLength("item", _ >= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val maxLength = new Check(CheckLevel.Error, "rule5")
        .hasMaxLength("item", _ <= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val patternMatch = new Check(CheckLevel.Error, "rule6").hasPattern("att2", "[a-z]".r)
      val min = new Check(CheckLevel.Error, "rule7").hasMin("val1", _ > 1)
      val max = new Check(CheckLevel.Error, "rule8").hasMax("val1", _ <= 3)
      val compliance = new Check(CheckLevel.Error, "rule9")
        .satisfies("item < 1000", "rule9", columns = List("item"))
      val areUniqueTrue = new Check(CheckLevel.Error, "rule10")
        .areUnique(Seq("item", "att1")) // att1 is not unique but is unique with item
      val areUniqueFalse = new Check(CheckLevel.Error, "rule11")
        .areUnique(Seq("att1", "att2")) // non unique for rows 1,4,6

      val expectedColumn1 = isComplete.description
      val expectedColumn2 = completeness.description
      val expectedColumn3 = isPrimaryKey.description
      val expectedColumn4 = minLength.description
      val expectedColumn5 = maxLength.description
      val expectedColumn6 = patternMatch.description
      val expectedColumn7 = min.description
      val expectedColumn8 = max.description
      val expectedColumn9 = compliance.description
      val expectedColumn10 = areUniqueTrue.description
      val expectedColumn11 = areUniqueFalse.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(isComplete)
        .addCheck(completeness)
        .addCheck(isPrimaryKey)
        .addCheck(minLength)
        .addCheck(maxLength)
        .addCheck(patternMatch)
        .addCheck(min)
        .addCheck(max)
        .addCheck(compliance)
        .addCheck(areUniqueTrue)
        .addCheck(areUniqueFalse)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data).orderBy("item")

      resultData.show()
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 + expectedColumn4 + expectedColumn5 +
          expectedColumn6 + expectedColumn7 + expectedColumn8 + expectedColumn9 + expectedColumn10 + expectedColumn11
      assert(resultData.columns.toSet == expectedColumns)


      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel2))

      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel3))

      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel4))

      val rowLevel5 = resultData.select(expectedColumn5).collect().map(r => r.getBoolean(0))
      assert(Seq(true, false, false, false, false, false).sameElements(rowLevel5))

      val rowLevel6 = resultData.select(expectedColumn6).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel6))

      val rowLevel7 = resultData.select(expectedColumn7).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(false, true, true, true, true, true).sameElements(rowLevel7))

      val rowLevel8 = resultData.select(expectedColumn8).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(true, true, true, false, false, false).sameElements(rowLevel8))

      val rowLevel9 = resultData.select(expectedColumn9).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(true, true, true, false, false, false).sameElements(rowLevel9))

      // Multiple Uniqueness for item and att1 - att1 is not unique but is unique with item
      val rowLevel10 = resultData.select(expectedColumn10).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel10))

      // Multiple Uniqueness for att1 and att2 - non unique for rows 1,4,6
      val rowLevel11 = resultData.select(expectedColumn11).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(false, true, true, false, true, false).sameElements(rowLevel11))
    }

    "generate a result that contains row-level results with true for filtered rows" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsWithIntId(session)

      val completeness = new Check(CheckLevel.Error, "rule1")
        .hasCompleteness("att2", _ > 0.7, None)
        .where("att1 = \"a\"")
      val uniqueness = new Check(CheckLevel.Error, "rule2")
        .hasUniqueness("att1", _ > 0.5, None)
      val uniquenessWhere = new Check(CheckLevel.Error, "rule3")
        .isUnique("att1")
        .where("item < 3")
      val min = new Check(CheckLevel.Error, "rule4")
        .hasMin("item", _ > 3, None)
        .where("item > 3")
      val max = new Check(CheckLevel.Error, "rule5")
        .hasMax("item", _ < 4, None)
        .where("item < 4")
      val patternMatch = new Check(CheckLevel.Error, "rule6")
        .hasPattern("att2", """(^f)""".r)
        .where("item < 4")
      val isPrimaryKey = new Check(CheckLevel.Error, "rule7")
        .isPrimaryKey("item")
        .where("item < 3")
      val uniqueValueRatio = new Check(CheckLevel.Error, "rule8")
        .hasUniqueValueRatio(Seq("att1"), _ >= 0.5)
        .where("item < 4")

      val expectedColumn1 = completeness.description
      val expectedColumn2 = uniqueness.description
      val expectedColumn3 = uniquenessWhere.description
      val expectedColumn4 = min.description
      val expectedColumn5 = max.description
      val expectedColumn6 = patternMatch.description
      val expectedColumn7 = isPrimaryKey.description
      val expectedColumn8 = uniqueValueRatio.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(completeness)
        .addCheck(uniqueness)
        .addCheck(uniquenessWhere)
        .addCheck(min)
        .addCheck(max)
        .addCheck(patternMatch)
        .addCheck(isPrimaryKey)
        .addCheck(uniqueValueRatio)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data).orderBy("item")
      resultData.show(false)
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 +
          expectedColumn4 + expectedColumn5 + expectedColumn6 + expectedColumn7 + expectedColumn8
      assert(resultData.columns.toSet == expectedColumns)

      // filtered rows 2,5 (where att1 = "a")
      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, false, true, true, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getAs[Any](0))
      assert(Seq(false, false, false, false, false, false).sameElements(rowLevel2))

      // filtered rows 3,4,5,6 (where item < 3)
      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel3))

      // filtered rows 1, 2, 3 (where item > 3)
      val minRowLevel = resultData.select(expectedColumn4).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, true, true, true).sameElements(minRowLevel))

      // filtered rows 4, 5, 6 (where item < 4)
      val maxRowLevel = resultData.select(expectedColumn5).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, true, true, true).sameElements(maxRowLevel))

      // filtered rows 4, 5, 6 (where item < 4)
      val rowLevel6 = resultData.select(expectedColumn6).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, false, false, true, true, true).sameElements(rowLevel6))

      // filtered rows 4, 5, 6 (where item < 4)
      val rowLevel7 = resultData.select(expectedColumn7).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel7))

      // filtered rows 4, 5, 6 (where item < 4) row 1 and 3 are the same -> not unique
      val rowLevel8 = resultData.select(expectedColumn8).collect().map(r => r.getAs[Any](0))
      assert(Seq(false, true, false, true, true, true).sameElements(rowLevel8))
    }

    "generate a result that contains row-level results with null for filtered rows" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsWithIntId(session)

      val analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL))

      val completeness = new Check(CheckLevel.Error, "rule1")
        .hasCompleteness("att2", _ > 0.7, None, analyzerOptions)
        .where("att1 = \"a\"")
      val uniqueness = new Check(CheckLevel.Error, "rule2")
        .hasUniqueness("att1", _ > 0.5, None, analyzerOptions)
      val uniquenessWhere = new Check(CheckLevel.Error, "rule3")
        .isUnique("att1", None, analyzerOptions)
        .where("item < 3")
      val min = new Check(CheckLevel.Error, "rule4")
        .hasMin("item", _ > 3, None, analyzerOptions)
        .where("item > 3")
      val max = new Check(CheckLevel.Error, "rule5")
        .hasMax("item", _ < 4, None, analyzerOptions)
        .where("item < 4")
      val patternMatch = new Check(CheckLevel.Error, "rule6")
        .hasPattern("att2", """(^f)""".r, analyzerOptions = analyzerOptions)
        .where("item < 4")
      val isPrimaryKey = new Check(CheckLevel.Error, "rule7")
        .isPrimaryKey("item", None, analyzerOptions = analyzerOptions)
        .where("item < 4")
      val uniqueValueRatio = new Check(CheckLevel.Error, "rule8")
        .hasUniqueValueRatio(Seq("att1"), _ >= 0.5, analyzerOptions = analyzerOptions)
        .where("item < 4")

      val expectedColumn1 = completeness.description
      val expectedColumn2 = uniqueness.description
      val expectedColumn3 = uniquenessWhere.description
      val expectedColumn4 = min.description
      val expectedColumn5 = max.description
      val expectedColumn6 = patternMatch.description
      val expectedColumn7 = isPrimaryKey.description
      val expectedColumn8 = uniqueValueRatio.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(completeness)
        .addCheck(uniqueness)
        .addCheck(uniquenessWhere)
        .addCheck(min)
        .addCheck(max)
        .addCheck(patternMatch)
        .addCheck(isPrimaryKey)
        .addCheck(uniqueValueRatio)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data).orderBy("item")
      resultData.show(false)
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 +
          expectedColumn4 + expectedColumn5  + expectedColumn6 + expectedColumn7 + expectedColumn8
      assert(resultData.columns.toSet == expectedColumns)

      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, null, false, true, null, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getAs[Any](0))
      assert(Seq(false, false, false, false, false, false).sameElements(rowLevel2))

      // filtered rows 3,4,5,6 (where item < 3)
      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, null, null, null, null).sameElements(rowLevel3))

      // filtered rows 1, 2, 3 (where item > 3)
      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.getAs[Any](0))
      assert(Seq(null, null, null, true, true, true).sameElements(rowLevel4))

      // filtered rows 4, 5, 6 (where item < 4)
      val rowLevel5 = resultData.select(expectedColumn5).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, null, null, null).sameElements(rowLevel5))

      // filtered rows 4, 5, 6 (where item < 4)
      val rowLevel6 = resultData.select(expectedColumn6).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, false, false, null, null, null).sameElements(rowLevel6))

      // filtered rows 4, 5, 6 (where item < 4)
      val rowLevel7 = resultData.select(expectedColumn7).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, null, null, null).sameElements(rowLevel7))

      // filtered rows 4, 5, 6 (where item < 4) row 1 and 3 are the same -> not unique
      val rowLevel8 = resultData.select(expectedColumn8).collect().map(r => r.getAs[Any](0))
      assert(Seq(false, true, false, null, null, null).sameElements(rowLevel8))
    }

    "generate a result that contains compliance row-level results " in withSparkSession { session =>
      val data = getDfWithNumericValues(session)
      val analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL))

      val complianceRange = new Check(CheckLevel.Error, "rule1")
        .isContainedIn("attNull", 0, 6, false, false)
      val complianceFilteredRange = new Check(CheckLevel.Error, "rule2")
        .isContainedIn("attNull", 0, 6, false, false)
        .where("att1 < 4")
      val complianceFilteredRangeNull = new Check(CheckLevel.Error, "rule3")
        .isContainedIn("attNull", 0, 6, false, false,
          analyzerOptions = analyzerOptions)
        .where("att1 < 4")
      val complianceInArray = new Check(CheckLevel.Error, "rule4")
        .isContainedIn("att2", Array("5", "6", "7"))
      val complianceInArrayFiltered = new Check(CheckLevel.Error, "rule5")
        .isContainedIn("att2", Array("5", "6", "7"))
        .where("att1 > 3")
      val complianceInArrayFilteredNull = new Check(CheckLevel.Error, "rule6")
        .isContainedIn("att2", Array("5", "6", "7"), Check.IsOne, None, analyzerOptions)
        .where("att1 > 3")

      val expectedColumn1 = complianceRange.description
      val expectedColumn2 = complianceFilteredRange.description
      val expectedColumn3 = complianceFilteredRangeNull.description
      val expectedColumn4 = complianceInArray.description
      val expectedColumn5 = complianceInArrayFiltered.description
      val expectedColumn6 = complianceInArrayFilteredNull.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(complianceRange)
        .addCheck(complianceFilteredRange)
        .addCheck(complianceFilteredRangeNull)
        .addCheck(complianceInArray)
        .addCheck(complianceInArrayFiltered)
        .addCheck(complianceInArrayFilteredNull)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data).orderBy("item")
      resultData.show(false)
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 +
          expectedColumn4 + expectedColumn5 + expectedColumn6
      assert(resultData.columns.toSet == expectedColumns)

      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, true, false, false).sameElements(rowLevel1))

      // filtered rows 4, 5, 6 (where att1 < 4) as true
      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel2))

      // filtered rows 4, 5, 6 (where att1 < 4) as null
      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, null, null, null).sameElements(rowLevel3))

      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.getAs[Any](0))
      assert(Seq(false, false, false, true, true, true).sameElements(rowLevel4))

      // filtered rows 1,2,3 (where att1 > 3) as true
      val rowLevel5 = resultData.select(expectedColumn5).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel5))

      // filtered rows 1,2,3 (where att1 > 3) as null
      val rowLevel6 = resultData.select(expectedColumn6).collect().map(r => r.getAs[Any](0))
      assert(Seq(null, null, null, true, true, true).sameElements(rowLevel6))
    }

    "generate a result that contains row-level results for null column values" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(session)

      val min = new Check(CheckLevel.Error, "rule1").hasMin("val2", _ > 2)
      val max = new Check(CheckLevel.Error, "rule2").hasMax("val2", _ <= 3)
      val patternMatchNullString = new Check(CheckLevel.Error, "rule3")
        .hasPattern("att2", """\w""".r)
      val complianceNullValue = new Check(CheckLevel.Error, "rule4")
        .satisfies("val2 > 3", "rule4", columns = List("val2"))

      val expectedColumn1 = min.description
      val expectedColumn2 = max.description
      val expectedColumn3 = patternMatchNullString.description
      val expectedColumn4 = complianceNullValue.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(min)
        .addCheck(max)
        .addCheck(patternMatchNullString)
        .addCheck(complianceNullValue)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)

      resultData.show()

      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 + expectedColumn4
      assert(resultData.columns.toSet == expectedColumns)

      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getAs[Any](0))
      assert(Seq(false, null, true, true, null, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, null, true, false, null, false).sameElements(rowLevel2))

      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel3))

      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.getAs[Any](0))
      assert(Seq(false, null, false, true, null, true).sameElements(rowLevel4))
    }

    "confirm that minLength and maxLength properly filters with nullBehavior empty" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(session)

      val minLength = new Check(CheckLevel.Error, "rule1")
        .hasMinLength("item", _ > 3,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString, FilteredRowOutcome.NULL)))
        .where("val1 > 3")
      val maxLength = new Check(CheckLevel.Error, "rule2")
        .hasMaxLength("item", _ <= 3,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString, FilteredRowOutcome.NULL)))
        .where("val1 < 4")

      val expectedColumn1 = minLength.description
      val expectedColumn2 = maxLength.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(minLength)
        .addCheck(maxLength)

      val result: VerificationResult = suite.run()

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)

      resultData.show(false)

      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2
      assert(resultData.columns.toSet == expectedColumns)

      // Unfiltered rows are all true - overall result should be Success
      assert(result.status == CheckStatus.Success)

      // minLength > 3 would fail for the first three rows (length 1,2,3)
      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getAs[Any](0))
      assert(Seq(null, null, null, true, true, true).sameElements(rowLevel1))

      // maxLength <= 3 would fail for the last three rows (length 4,5,6)
      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, null, null, null).sameElements(rowLevel2))
    }

    "generate a result that contains length row-level results with nullBehavior fail" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(session)

      val minLength = new Check(CheckLevel.Error, "rule1")
        .hasMinLength("att2", _ >= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val maxLength = new Check(CheckLevel.Error, "rule2")
        .hasMaxLength("att2", _ <= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      // filtered rows as null
      val minLengthFilterNull = new Check(CheckLevel.Error, "rule3")
        .hasMinLength("att2", _ >= 1,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail, FilteredRowOutcome.NULL)))
        .where("val1 < 5")
      val maxLengthFilterNull = new Check(CheckLevel.Error, "rule4")
        .hasMaxLength("att2", _ <= 1,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail, FilteredRowOutcome.NULL)))
        .where("val1 < 5")
      val minLengthFilterTrue = new Check(CheckLevel.Error, "rule5")
        .hasMinLength("att2", _ >= 1,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail, FilteredRowOutcome.TRUE)))
        .where("val1 < 5")
      val maxLengthFilterTrue = new Check(CheckLevel.Error, "rule6")
        .hasMaxLength("att2", _ <= 1,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail, FilteredRowOutcome.TRUE)))
        .where("val1 < 5")
      val expectedColumn1 = minLength.description
      val expectedColumn2 = maxLength.description
      val expectedColumn3 = minLengthFilterNull.description
      val expectedColumn4 = maxLengthFilterNull.description
      val expectedColumn5 = minLengthFilterTrue.description
      val expectedColumn6 = maxLengthFilterTrue.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(minLength)
        .addCheck(maxLength)
        .addCheck(minLengthFilterNull)
        .addCheck(maxLengthFilterNull)
        .addCheck(minLengthFilterTrue)
        .addCheck(maxLengthFilterTrue)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)

      resultData.show()
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 +
          expectedColumn4 + expectedColumn5 + expectedColumn6
      assert(resultData.columns.toSet == expectedColumns)

      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel2))

      // filtered last two rows where(val1 < 5)
      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, false, true, null, null).sameElements(rowLevel3))

      // filtered last two rows where(val1 < 5)
      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, false, true, null, null).sameElements(rowLevel4))

      // filtered last two rows where(val1 < 5)
      val rowLevel5 = resultData.select(expectedColumn5).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, false, true, true, true).sameElements(rowLevel5))

      // filtered last two rows where(val1 < 5)
      val rowLevel6 = resultData.select(expectedColumn6).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, false, true, true, true).sameElements(rowLevel6))
    }

    "generate a result that contains length row-level results with nullBehavior empty" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(session)

      // null should fail since length 0 is not >= 1
      val minLength = new Check(CheckLevel.Error, "rule1")
        .hasMinLength("att2", _ >= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString)))
      // nulls should succeed since length 0 is < 2
      val maxLength = new Check(CheckLevel.Error, "rule2")
        .hasMaxLength("att2", _ < 2, analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString)))
      // filtered rows as null
      val minLengthFilterNull = new Check(CheckLevel.Error, "rule3")
        .hasMinLength("att2", _ >= 1,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString, FilteredRowOutcome.NULL)))
        .where("val1 < 5")
      val maxLengthFilterNull = new Check(CheckLevel.Error, "rule4")
        .hasMaxLength("att2", _ < 2,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString, FilteredRowOutcome.NULL)))
        .where("val1 < 5")
      val minLengthFilterTrue = new Check(CheckLevel.Error, "rule5")
        .hasMinLength("att2", _ >= 1,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString, FilteredRowOutcome.TRUE)))
        .where("val1 < 5")
      val maxLengthFilterTrue = new Check(CheckLevel.Error, "rule6")
        .hasMaxLength("att2", _ < 2,
          analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString, FilteredRowOutcome.TRUE)))
        .where("val1 < 5")

      val expectedColumn1 = minLength.description
      val expectedColumn2 = maxLength.description
      val expectedColumn3 = minLengthFilterNull.description
      val expectedColumn4 = maxLengthFilterNull.description
      val expectedColumn5 = minLengthFilterTrue.description
      val expectedColumn6 = maxLengthFilterTrue.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(minLength)
        .addCheck(maxLength)
        .addCheck(minLengthFilterNull)
        .addCheck(maxLengthFilterNull)
        .addCheck(minLengthFilterTrue)
        .addCheck(maxLengthFilterTrue)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)

      resultData.show()
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 +
          expectedColumn4 + expectedColumn5 + expectedColumn6
      assert(resultData.columns.toSet == expectedColumns)


      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel2))

      // filtered last two rows where(val1 < 5)
      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, false, true, null, null).sameElements(rowLevel3))

      // filtered last two rows where(val1 < 5)
      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, true, null, null).sameElements(rowLevel4))

      // filtered last two rows where(val1 < 5)
      val rowLevel5 = resultData.select(expectedColumn5).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, false, true, true, true).sameElements(rowLevel5))

      // filtered last two rows where(val1 < 5)
      val rowLevel6 = resultData.select(expectedColumn6).collect().map(r => r.getAs[Any](0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel6))
    }

    "accept analysis config for mandatory analysis" in withSparkSession { sparkSession =>

      import sparkSession.implicits._
      val df = getDfFull(sparkSession)

      val result = {
        val checkToSucceed = Check(CheckLevel.Error, "group-1")
          .isComplete("att1") // 1.0
          .hasCompleteness("att1", _ == 1.0) // 1.0

        val analyzers = Size() :: // Analyzer that works on overall document
          Completeness("att2") ::
          Uniqueness("att2") :: // Analyzer that works on single column
          MutualInformation("att1", "att2") :: Nil // Analyzer that works on multi column

        VerificationSuite().onData(df).addCheck(checkToSucceed)
          .addRequiredAnalyzers(analyzers).run()
      }

      assert(result.status == CheckStatus.Success)

      val analysisDf = AnalyzerContext.successMetricsAsDataFrame(sparkSession,
        AnalyzerContext(result.metrics))

      val expected = Seq(
        ("Dataset", "*", "Size", 4.0),
        ("Column", "att1", "Completeness", 1.0),
        ("Column", "att2", "Completeness", 1.0),
        ("Column", "att2", "Uniqueness", 0.25),
        ("Multicolumn", "att1,att2", "MutualInformation",
          -(0.75 * math.log(0.75) + 0.25 * math.log(0.25))))
        .toDF("entity", "instance", "name", "value")


      assertSameRows(analysisDf, expected)

    }

    "accept analysis config for mandatory analysis for checks with filters" in withSparkSession { sparkSession =>

      import sparkSession.implicits._
      val df = getDfCompleteAndInCompleteColumns(sparkSession)

      val result = {
        val checkToSucceed = Check(CheckLevel.Error, "group-1")
          .hasCompleteness("att2", _ > 0.7, null) // 0.75
          .where("att1 = \"a\"")
        val uniquenessCheck = Check(CheckLevel.Error, "group-2")
          .isUnique("att1")
          .where("item < 3")


        VerificationSuite().onData(df).addCheck(checkToSucceed).addCheck(uniquenessCheck).run()
      }

      assert(result.status == CheckStatus.Success)

      val analysisDf = AnalyzerContext.successMetricsAsDataFrame(sparkSession,
        AnalyzerContext(result.metrics))

      val expected = Seq(
        ("Column", "att2", "Completeness (where: att1 = \"a\")", 0.75),
        ("Column", "att1", "Uniqueness (where: item < 3)", 1.0))
        .toDF("entity", "instance", "name", "value")


      assertSameRows(analysisDf, expected)

    }

    "run the analysis even there are no constraints" in withSparkSession { sparkSession =>

      import sparkSession.implicits._
      val df = getDfFull(sparkSession)

      VerificationSuite().onData(df).addRequiredAnalyzer(Size()).run() match {
        case result =>
          assert(result.status == CheckStatus.Success)

          val analysisDf = AnalyzerContext.successMetricsAsDataFrame(sparkSession,
              AnalyzerContext(result.metrics))

          val expected = Seq(
            ("Dataset", "*", "Size", 4.0)
          ).toDF("entity", "instance", "name", "value")

          assertSameRows(analysisDf, expected)
      }
    }

    "reuse existing results" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = getDfWithNumericValues(sparkSession)

        val analyzerToTestReusingResults = Distinctness(Seq("att1", "att2"))

        val verificationResult = VerificationSuite().onData(df)
          .addRequiredAnalyzer(analyzerToTestReusingResults).run()
        val analysisResult = AnalyzerContext(verificationResult.metrics)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)
        repository.save(resultKey, analysisResult)

        val analyzers = analyzerToTestReusingResults :: Uniqueness(Seq("att2", "item")) :: Nil

        val (separateResults, numSeparateJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = analyzers.map { _.calculate(df) }.toSet
          (results, stat.jobCount)
        }

        val (runnerResults, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = VerificationSuite().onData(df).useRepository(repository)
            .reuseExistingResultsForKey(resultKey).addRequiredAnalyzers(analyzers).run()
            .metrics.values.toSet

          (results, stat.jobCount)
        }

        assert(numSeparateJobs == analyzers.length * 2)
        assert(numCombinedJobs == 2)
        assert(separateResults.toString == runnerResults.toString)
      }

    "save results if specified" in
      withSparkSession { sparkSession =>

        val df = getDfWithNumericValues(sparkSession)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        val metrics = VerificationSuite().onData(df).useRepository(repository)
          .addRequiredAnalyzers(analyzers).saveOrAppendResult(resultKey).run().metrics

        val analyzerContext = AnalyzerContext(metrics)

        assert(analyzerContext == repository.loadByKey(resultKey).get)
      }

    "only append results to repository without unnecessarily overwriting existing ones" in
      withSparkSession { sparkSession =>

        val df = getDfWithNumericValues(sparkSession)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        val completeMetricResults = VerificationSuite().onData(df).useRepository(repository)
          .addRequiredAnalyzers(analyzers).saveOrAppendResult(resultKey).run().metrics

        val completeAnalyzerContext = AnalyzerContext(completeMetricResults)

        // Calculate and save results for first analyzer
        VerificationSuite().onData(df).useRepository(repository)
          .addRequiredAnalyzer(Size()).saveOrAppendResult(resultKey).run()

        // Calculate and append results for second analyzer
        VerificationSuite().onData(df).useRepository(repository)
          .addRequiredAnalyzer(Completeness("item"))
          .saveOrAppendResult(resultKey).run()

        assert(completeAnalyzerContext == repository.loadByKey(resultKey).get)
      }

    "if there are previous results in the repository new results should pre preferred in case of " +
      "conflicts" in withSparkSession { sparkSession =>

        val df = getDfWithNumericValues(sparkSession)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        val actualResult = VerificationSuite().onData(df).useRepository(repository)
            .addRequiredAnalyzers(analyzers).run()
        val expectedAnalyzerContextOnLoadByKey = AnalyzerContext(actualResult.metrics)

        val resultWhichShouldBeOverwritten = AnalyzerContext(Map(Size() -> DoubleMetric(
          Entity.Dataset, "", "", util.Try(100.0))))

        repository.save(resultKey, resultWhichShouldBeOverwritten)

        // This should overwrite the previous Size value
        VerificationSuite().onData(df).useRepository(repository)
          .addRequiredAnalyzers(analyzers).saveOrAppendResult(resultKey).run()

        assert(expectedAnalyzerContextOnLoadByKey == repository.loadByKey(resultKey).get)
    }

    "addAnomalyCheck should work" in withSparkSession { sparkSession =>
      evaluateWithRepositoryWithHistory { repository =>

        val df = getDfWithNRows(sparkSession, 11)
        val saveResultsWithKey = ResultKey(5, Map.empty)

        val analyzers = Completeness("item") :: Nil

        val verificationResultOne = VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .saveOrAppendResult(saveResultsWithKey)
          .addAnomalyCheck(
            AbsoluteChangeStrategy(Some(-2.0), Some(2.0)),
            Size(),
            Some(AnomalyCheckConfig(CheckLevel.Warning, "Anomaly check to fail"))
          )
          .run()

        val verificationResultTwo = VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .saveOrAppendResult(saveResultsWithKey)
          .addAnomalyCheck(
            AbsoluteChangeStrategy(Some(-7.0), Some(7.0)),
            Size(),
            Some(AnomalyCheckConfig(CheckLevel.Error, "Anomaly check to succeed",
              Map.empty, Some(0), Some(11)))
          )
          .run()

        val verificationResultThree = VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .saveOrAppendResult(saveResultsWithKey)
          .addAnomalyCheck(
            AbsoluteChangeStrategy(Some(-7.0), Some(7.0)),
            Size()
          )
          .run()

        val checkResultsOne = verificationResultOne.checkResults.head._2.status
        val checkResultsTwo = verificationResultTwo.checkResults.head._2.status
        val checkResultsThree = verificationResultThree.checkResults.head._2.status

        assert(checkResultsOne == CheckStatus.Warning)
        assert(checkResultsTwo == CheckStatus.Success)
        assert(checkResultsThree == CheckStatus.Success)
      }
    }

    "addAnomalyCheck with duplicate check analyzer should work" in
      withSparkSession { sparkSession =>
      evaluateWithRepositoryWithHistory { repository =>

        val df = getDfWithNRows(sparkSession, 11)
        val saveResultsWithKey = ResultKey(5, Map.empty)

        val analyzers = Completeness("item") :: Nil

        val verificationResultOne = VerificationSuite()
          .onData(df)
          .addCheck(Check(CheckLevel.Error, "group-1").hasSize(_ == 11))
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .saveOrAppendResult(saveResultsWithKey)
          .addAnomalyCheck(
            AbsoluteChangeStrategy(Some(-2.0), Some(2.0)),
            Size(),
            Some(AnomalyCheckConfig(CheckLevel.Warning, "Anomaly check to fail"))
          )
          .run()

        val verificationResultTwo = VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .saveOrAppendResult(saveResultsWithKey)
          .addAnomalyCheck(
            AbsoluteChangeStrategy(Some(-7.0), Some(7.0)),
            Size(),
            Some(AnomalyCheckConfig(CheckLevel.Error, "Anomaly check to succeed",
              Map.empty, Some(0), Some(11)))
          )
          .run()

        val checkResultsOne = verificationResultOne.checkResults.values.toSeq(1).status
        val checkResultsTwo = verificationResultTwo.checkResults.head._2.status

        assert(checkResultsOne == CheckStatus.Warning)
        assert(checkResultsTwo == CheckStatus.Success)
      }
    }

    "write output files to specified locations" in withSparkSession { sparkSession =>

      val df = getDfWithNumericValues(sparkSession)

      val checkToSucceed = Check(CheckLevel.Error, "group-1")
        .isComplete("att1") // 1.0
        .hasCompleteness("att1", _ == 1.0) // 1.0

      val tempDir = TempFileUtils.tempDir("verificationOuput")
      val checkResultsPath = tempDir + "/check-result.json"
      val successMetricsPath = tempDir + "/success-metrics.json"

      VerificationSuite().onData(df)
        .addCheck(checkToSucceed)
        .useSparkSession(sparkSession)
        .saveCheckResultsJsonToPath(checkResultsPath)
        .saveSuccessMetricsJsonToPath(successMetricsPath)
        .run()

      DfsUtils.readFromFileOnDfs(sparkSession, checkResultsPath) {
        inputStream => assert(inputStream.read() > 0)
      }
      DfsUtils.readFromFileOnDfs(sparkSession, successMetricsPath) {
        inputStream => assert(inputStream.read() > 0)
      }
    }

    "call state persister if specified" in withSparkSession { sparkSession =>
      val statePersister = mock[StatePersister]

      val df = getDfWithNumericValues(sparkSession)
      val completeness = Completeness("att1")
      val analyzers = Sum("att2") :: completeness :: Nil
      val states = SumState(18.0) :: NumMatchesAndCount(6, 6, Some(completeness.criterion)) :: Nil

      analyzers.zip(states).foreach { case (analyzer: Analyzer[_, _], state: State[_]) =>
        (statePersister.persist[state.type] _)
          .expects(
            where { (analyzer_, state_) => analyzer_ == analyzer && state_ == state }
          )
          .returns()
          .atLeastOnce()
      }

      VerificationSuite().onData(df)
        .addRequiredAnalyzers(analyzers)
        .saveStatesWith(statePersister)
        .run()
    }

    "load stored states for aggregation if specified" in withSparkSession { sparkSession =>
      val stateLoaderStub = stub[StateLoader]

      val df = getDfWithNumericValues(sparkSession)
      val analyzers = Sum("att2") :: Completeness("att1") :: Nil

      (stateLoaderStub.load[SumState] _)
        .when(Sum("att2"))
        .returns(Some(SumState(18.0)))

      (stateLoaderStub.load[NumMatchesAndCount] _)
        .when(Completeness("att1"))
        .returns(Some(NumMatchesAndCount(0, 6)))


      val results = VerificationSuite().onData(df)
        .addRequiredAnalyzers(analyzers)
        .aggregateWith(stateLoaderStub)
        .run()

      assert(results.metrics(Sum("att2")).value.get == 18.0 * 2)
      assert(results.metrics(Completeness("att1")).value.get == 0.5)
    }

    "keep order of check constraints and their results" in withSparkSession { sparkSession =>

      val df = getDfWithNumericValues(sparkSession)

      val expectedConstraints = Seq(
        Constraint.completenessConstraint("att1", _ == 1.0),
        Constraint.complianceConstraint("att1 is positive", "att1", _ == 1.0, columns = List("att1"))
      )

      val check = expectedConstraints.foldLeft(Check(CheckLevel.Error, "check")) {
        case (currentCheck, constraint) => currentCheck.addConstraint(constraint)
      }

      // constraints are added in expected order to Check object
      assert(check.constraints == expectedConstraints)
      assert(check.constraints != expectedConstraints.reverse)

      val results = VerificationSuite().onData(df)
        .addCheck(check)
        .run()

      val checkConstraintsWithResultConstraints = check.constraints.zip(
        results.checkResults(check).constraintResults)

      checkConstraintsWithResultConstraints.foreach {
        case (checkConstraint, checkResultConstraint) =>
          assert(checkConstraint == checkResultConstraint.constraint)
      }
    }

    "A well-defined check should pass even if an ill-defined check is also configured quotes" in withSparkSession {
      sparkSession =>
        val df = getDfWithDistinctValuesQuotes(sparkSession)

        val rangeCheck = Check(CheckLevel.Error, "a")
          .isContainedIn("att2", Array("can't", "help", "but", "wouldn't"))

        val reasonCheck = Check(CheckLevel.Error, "a")
          .isContainedIn("reason", Array("Already Has ", " Can't Proceed"))

        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(rangeCheck)
          .addCheck(reasonCheck)
          .run()

        val checkSuccessResult = verificationResult.checkResults(rangeCheck)
        checkSuccessResult.constraintResults.map(_.message) shouldBe List(None)
        println(checkSuccessResult.constraintResults.map(_.message))
        assert(checkSuccessResult.status == CheckStatus.Success)

        val reasonResult = verificationResult.checkResults(reasonCheck)
        checkSuccessResult.constraintResults.map(_.message) shouldBe List(None)
        println(checkSuccessResult.constraintResults.map(_.message))
        assert(checkSuccessResult.status == CheckStatus.Success)
    }

    "A well-defined check should pass even if an ill-defined check is also configured" in withSparkSession {
      sparkSession =>
        val df = getDfWithNameAndAge(sparkSession)

        val checkThatShouldSucceed =
          Check(CheckLevel.Error, "shouldSucceedForValue").isComplete("name")

        val isCompleteCheckThatShouldFailCompleteness = Check(CheckLevel.Error, "shouldErrorStringType")
          .isComplete("fake")

        val complianceCheckThatShouldSucceed =
          Check(CheckLevel.Error, "shouldSucceedForAge").isContainedIn("age", 1, 100)

        val complianceCheckThatShouldFailForAge =
          Check(CheckLevel.Error, "shouldFailForAge").isContainedIn("age", 1, 19)

        val checkThatShouldFail = Check(CheckLevel.Error, "shouldErrorColumnNA")
          .isContainedIn("fakeColumn", 10, 90)

        val complianceCheckThatShouldFail = Check(CheckLevel.Error, "shouldErrorStringType")
          .isContainedIn("name", 1, 3)

        val complianceCheckThatShouldFailCompleteness = Check(CheckLevel.Error, "shouldErrorStringType")
          .hasCompleteness("fake", x => x > 0)

        val checkHasDataInSyncTest = Check(CheckLevel.Error, "shouldSucceedForAge")
          .doesDatasetMatch(df, Map("age" -> "age"), _ > 0.99, hint = Some("shouldPass"))

        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(checkThatShouldSucceed)
          .addCheck(isCompleteCheckThatShouldFailCompleteness)
          .addCheck(complianceCheckThatShouldSucceed)
          .addCheck(complianceCheckThatShouldFailForAge)
          .addCheck(checkThatShouldFail)
          .addCheck(complianceCheckThatShouldFail)
          .addCheck(complianceCheckThatShouldFailCompleteness)
          .addCheck(checkHasDataInSyncTest)
          .run()

        val checkSuccessResult = verificationResult.checkResults(checkThatShouldSucceed)
        checkSuccessResult.constraintResults.map(_.message) shouldBe List(None)
        assert(checkSuccessResult.status == CheckStatus.Success)

        val checkIsCompleteFailedResult = verificationResult.checkResults(isCompleteCheckThatShouldFailCompleteness)
        checkIsCompleteFailedResult.constraintResults.map(_.message) shouldBe
          List(Some("Input data does not include column fake!"))
        assert(checkIsCompleteFailedResult.status == CheckStatus.Error)

        val checkAgeSuccessResult = verificationResult.checkResults(complianceCheckThatShouldSucceed)
        checkAgeSuccessResult.constraintResults.map(_.message) shouldBe List(None)
        assert(checkAgeSuccessResult.status == CheckStatus.Success)

        val checkFailedResult = verificationResult.checkResults(checkThatShouldFail)
        checkFailedResult.constraintResults.map(_.message) shouldBe
          List(Some("Input data does not include column fakeColumn!"))
        assert(checkFailedResult.status == CheckStatus.Error)

        val checkFailedResultStringType = verificationResult.checkResults(complianceCheckThatShouldFail)
        checkFailedResultStringType.constraintResults.map(_.message) shouldBe
          List(Some("Empty state for analyzer Compliance(name between 1.0 and 3.0,`name`" +
            " IS NULL OR (`name` >= 1.0 AND `name` <= 3.0)," +
            "None,List(name),None), all input values were NULL."))
        assert(checkFailedResultStringType.status == CheckStatus.Error)

        val checkFailedCompletenessResult = verificationResult.checkResults(complianceCheckThatShouldFailCompleteness)
        checkFailedCompletenessResult.constraintResults.map(_.message) shouldBe
          List(Some("Input data does not include column fake!"))
        assert(checkFailedCompletenessResult.status == CheckStatus.Error)

        val checkDataSyncResult = verificationResult.checkResults(checkHasDataInSyncTest)
        checkDataSyncResult.status shouldBe CheckStatus.Success
    }

    "Well-defined checks should produce correct result even if another check throws an exception" in withSparkSession {
      sparkSession =>
        val df = getDfWithNameAndAge(sparkSession)


        val checkThatShouldSucceed =
          Check(CheckLevel.Error, "shouldSucceedForValue").isComplete("name")


        val checkThatWillThrow = Check(CheckLevel.Error, "shouldThrow")
          .hasSize(_ => {
            throw new IllegalArgumentException("borked")
          })

        val complianceCheckThatShouldSucceed =
          Check(CheckLevel.Error, "shouldSucceedForAge").isContainedIn("age", 1, 100)


        val isCompleteCheckThatShouldFailCompleteness = Check(CheckLevel.Error, "shouldErrorStringType")
          .isComplete("fake")

        val verificationResult = VerificationSuite()
            .onData(df)
            .addCheck(checkThatShouldSucceed)
            .addCheck(checkThatWillThrow)
            .addCheck(isCompleteCheckThatShouldFailCompleteness)
            .addCheck(complianceCheckThatShouldSucceed)
            .run()

        val checkSuccessResult = verificationResult.checkResults(checkThatShouldSucceed)
        checkSuccessResult.constraintResults.map(_.message) shouldBe List(None)
        assert(checkSuccessResult.status == CheckStatus.Success)

        val checkExceptionResult = verificationResult.checkResults(checkThatWillThrow)
        checkExceptionResult.constraintResults.map(_.message) shouldBe
          List(Some("Can't execute the assertion: borked!"))
        assert(checkExceptionResult.status == CheckStatus.Error)

        val checkIsCompleteFailedResult = verificationResult.checkResults(isCompleteCheckThatShouldFailCompleteness)
        checkIsCompleteFailedResult.constraintResults.map(_.message) shouldBe
          List(Some("Input data does not include column fake!"))
        assert(checkIsCompleteFailedResult.status == CheckStatus.Error)

        val checkAgeSuccessResult = verificationResult.checkResults(complianceCheckThatShouldSucceed)
        checkAgeSuccessResult.constraintResults.map(_.message) shouldBe List(None)
        assert(checkAgeSuccessResult.status == CheckStatus.Success)

    }

    "A well-defined completeness check should pass even with a single column" in withSparkSession {
      sparkSession =>
        val df = getDfWithVariableStringLengthValues(sparkSession)

        val checkThatShouldSucceed =
          Check(CheckLevel.Error, "shouldSucceedForValue").isComplete("att1")

        val checkThatShouldFail = Check(CheckLevel.Error, "shouldErrorStringType")
          .isComplete("fake")

        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(checkThatShouldSucceed)
          .addCheck(checkThatShouldFail)
          .run()

        val checkSuccessResult = verificationResult.checkResults(checkThatShouldSucceed)
        checkSuccessResult.constraintResults.map(_.message) shouldBe List(None)
        assert(checkSuccessResult.status == CheckStatus.Success)

        val checkFailedResult = verificationResult.checkResults(checkThatShouldFail)
        checkFailedResult.constraintResults.map(_.message) shouldBe
          List(Some("Input data does not include column fake!"))
        assert(checkFailedResult.status == CheckStatus.Error)
    }

    "The data check contains data with escape character including all the special characters" in withSparkSession {
      sparkSession =>

        val df = getDfWithEscapeCharacters(sparkSession)

        val nameData = Seq("'foo'", "Yes This's My Name", "It's foo", "foo", "foo '' name", "'''",
          "", "Trying !o include: @ll the #$peci@l charac%ers possib^e & test* that (out)~[here] {which} i`s great?\";")

        val nameDataSubset = Seq("")

        val ageData = Seq(22, 25, 29, 33, 50).map(_.toString)

        val nameCheckThatShouldSucceed =
          Check(CheckLevel.Error, "shouldSucceedForName").isContainedIn("name", nameData.toArray)

        val ageCheckThatShouldSucceed =
          Check(CheckLevel.Error, "shouldSucceedForAge").isContainedIn("age", ageData.toArray)

        val nameCheckThatShouldFail =
          Check(CheckLevel.Error, "shouldFailForEmptyName").isContainedIn("name", Seq.empty[String].toArray)

        val subsetNameCheckThatShouldFail =
          Check(CheckLevel.Error, "shouldFailForSubsetNameList").isContainedIn("name", nameDataSubset.toArray)

        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(nameCheckThatShouldSucceed)
          .addCheck(ageCheckThatShouldSucceed)
          .addCheck(nameCheckThatShouldFail)
          .addCheck(subsetNameCheckThatShouldFail)
          .run()

        val checkNameResult = verificationResult.checkResults(nameCheckThatShouldSucceed)
        checkNameResult.constraintResults.map(_.message) shouldBe List(None)
        assert(checkNameResult.status == CheckStatus.Success)

        val checkAgeResult = verificationResult.checkResults(ageCheckThatShouldSucceed)
        checkAgeResult.constraintResults.map(_.message) shouldBe List(None)
        assert(checkAgeResult.status == CheckStatus.Success)

        val checkNameFailResult = verificationResult.checkResults(nameCheckThatShouldFail)
        checkNameFailResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: 0.125 does not meet the constraint requirement!"))
        assert(checkNameFailResult.status == CheckStatus.Error)

        val subsetNameFailResult = verificationResult.checkResults(subsetNameCheckThatShouldFail)
        subsetNameFailResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: 0.125 does not meet the constraint requirement!"))
        assert(subsetNameFailResult.status == CheckStatus.Error)
    }

    "Should work Data Synchronization checks for single column" in withSparkSession {
      sparkSession =>
        val df = getDateDf(sparkSession).select("id", "product", "product_id", "units")
        val dfModified = df.withColumn("id", when(col("id") === 100, 99)
          .otherwise(col("id")))
        val dfColRenamed = df.withColumnRenamed("id", "id_renamed")

        val dataSyncCheckPass = Check(CheckLevel.Error, "data synchronization check pass")
          .doesDatasetMatch(dfModified, Map("id" -> "id"), _ > 0.7, hint = Some("shouldPass"))

        val dataSyncCheckFail = Check(CheckLevel.Error, "data synchronization check fail")
          .doesDatasetMatch(dfModified, Map("id" -> "id"), _ > 0.9, hint = Some("shouldFail"))

        val emptyDf = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], df.schema)
        val dataSyncCheckEmpty = Check(CheckLevel.Error, "data synchronization check on empty DataFrame")
          .doesDatasetMatch(emptyDf, Map("id" -> "id"), _ < 0.5)

        val dataSyncCheckColMismatchDestination =
          Check(CheckLevel.Error, "data synchronization check col mismatch in destination")
            .doesDatasetMatch(dfModified, Map("id" -> "id2"), _ < 0.5)

        val dataSyncCheckColMismatchSource =
          Check(CheckLevel.Error, "data synchronization check col mismatch in source")
            .doesDatasetMatch(dfModified, Map("id2" -> "id"), _ < 0.5)

        val dataSyncCheckColRenamed =
          Check(CheckLevel.Error, "data synchronization check col names renamed")
            .doesDatasetMatch(dfColRenamed, Map("id" -> "id_renamed"), _ == 1.0)

        val dataSyncFullMatch =
          Check(CheckLevel.Error, "data synchronization check full match")
            .doesDatasetMatch(df, Map("id" -> "id"), _ == 1.0)


        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(dataSyncCheckPass)
          .addCheck(dataSyncCheckFail)
          .addCheck(dataSyncCheckEmpty)
          .addCheck(dataSyncCheckColMismatchDestination)
          .addCheck(dataSyncCheckColMismatchSource)
          .addCheck(dataSyncCheckColRenamed)
          .addCheck(dataSyncFullMatch)
          .run()

        val passResult = verificationResult.checkResults(dataSyncCheckPass)
        passResult.constraintResults.map(_.message) shouldBe
          List(None)
        assert(passResult.status == CheckStatus.Success)

        val failResult = verificationResult.checkResults(dataSyncCheckFail)
        failResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: 0.8 does not meet the constraint requirement! shouldFail"))
        assert(failResult.status == CheckStatus.Error)

        val emptyResult = verificationResult.checkResults(dataSyncCheckEmpty)
        emptyResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: NaN does not meet the constraint requirement!"))
        assert(emptyResult.status == CheckStatus.Error)

        val colMismatchDestResult = verificationResult.checkResults(dataSyncCheckColMismatchDestination)
        colMismatchDestResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: NaN does not meet the constraint requirement!"))
        assert(colMismatchDestResult.status == CheckStatus.Error)

        val colMismatchSourceResult = verificationResult.checkResults(dataSyncCheckColMismatchSource)
        colMismatchSourceResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: NaN does not meet the constraint requirement!"))
        assert(colMismatchSourceResult.status == CheckStatus.Error)

        val colRenamedResult = verificationResult.checkResults(dataSyncCheckColRenamed)
        colRenamedResult.constraintResults.map(_.message) shouldBe List(None)
        assert(colRenamedResult.status == CheckStatus.Success)

        val fullMatchResult = verificationResult.checkResults(dataSyncFullMatch)
        fullMatchResult.constraintResults.map(_.message) shouldBe List(None)
        assert(fullMatchResult.status == CheckStatus.Success)

    }

    "Should work Data Synchronization checks for multiple column" in withSparkSession {
      sparkSession =>
        val df = getDateDf(sparkSession).select("id", "product", "product_id", "units")
        val dfModified = df.withColumn("id", when(col("id") === 100, 99)
          .otherwise(col("id")))
        val dfColRenamed = df.withColumnRenamed("id", "id_renamed")
        val colMap = Map("id" -> "id", "product" -> "product")

        // Additional DataFrames for testing matchColumnMappings
        val dfWithAdditionalColumns = df.withColumn("newColumn", lit(1))

        val matchColMap = Map("product" -> "product")
        val dataSyncCheckWithMatchColumns = Check(CheckLevel.Error,
          "data synchronization check with matchColumnMappings")
          .doesDatasetMatch(df, colMap, _ > 0.7, Some(matchColMap),
            hint = Some("Check with matchColumnMappings"))

        val dataSyncCheckWithAdditionalCols = Check(CheckLevel.Error,
          "data synchronization check with additional columns")
          .doesDatasetMatch(dfWithAdditionalColumns, colMap, _ > 0.7, Some(matchColMap),
            hint = Some("Check with additional columns and matchColumnMappings"))

        val dataSyncCheckPass = Check(CheckLevel.Error, "data synchronization check")
          .doesDatasetMatch(dfModified, colMap, _ > 0.7, hint = Some("shouldPass"))

        val dataSyncCheckFail = Check(CheckLevel.Error, "data synchronization check")
          .doesDatasetMatch(dfModified, colMap, _ > 0.9, hint = Some("shouldFail"))

        val emptyDf = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], df.schema)
        val dataSyncCheckEmpty = Check(CheckLevel.Error, "data synchronization check on empty DataFrame")
          .doesDatasetMatch(emptyDf, colMap, _ < 0.5)

        val dataSyncCheckColMismatchDestination =
          Check(CheckLevel.Error, "data synchronization check col mismatch in destination")
            .doesDatasetMatch(dfModified, colMap, _ > 0.9)

        val dataSyncCheckColMismatchSource =
          Check(CheckLevel.Error, "data synchronization check col mismatch in source")
            .doesDatasetMatch(dfModified, Map("id2" -> "id", "product" -> "product"), _ < 0.5)

        val dataSyncCheckColRenamed =
          Check(CheckLevel.Error, "data synchronization check col names renamed")
            .doesDatasetMatch(dfColRenamed, Map("id" -> "id_renamed", "product" -> "product"), _ == 1.0,
              hint = Some("shouldPass"))

        val dataSyncFullMatch =
          Check(CheckLevel.Error, "data synchronization check col full match")
            .doesDatasetMatch(df, colMap, _ == 1, hint = Some("shouldPass"))


        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(dataSyncCheckPass)
          .addCheck(dataSyncCheckFail)
          .addCheck(dataSyncCheckEmpty)
          .addCheck(dataSyncCheckColMismatchDestination)
          .addCheck(dataSyncCheckColMismatchSource)
          .addCheck(dataSyncCheckColRenamed)
          .addCheck(dataSyncFullMatch)
          .addCheck(dataSyncCheckWithMatchColumns)
          .addCheck(dataSyncCheckWithAdditionalCols)
          .run()

        val passResult = verificationResult.checkResults(dataSyncCheckPass)
        passResult.constraintResults.map(_.message) shouldBe
          List(None)
        assert(passResult.status == CheckStatus.Success)

        val failResult = verificationResult.checkResults(dataSyncCheckFail)
        failResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: 0.8 does not meet the constraint requirement! shouldFail"))
        assert(failResult.status == CheckStatus.Error)

        val emptyResult = verificationResult.checkResults(dataSyncCheckEmpty)
        emptyResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: NaN does not meet the constraint requirement!"))
        assert(emptyResult.status == CheckStatus.Error)

        val colMismatchDestResult = verificationResult.checkResults(dataSyncCheckColMismatchDestination)
        colMismatchDestResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: 0.8 does not meet the constraint requirement!"))
        assert(colMismatchDestResult.status == CheckStatus.Error)

        val colMismatchSourceResult = verificationResult.checkResults(dataSyncCheckColMismatchSource)
        colMismatchSourceResult.constraintResults.map(_.message) shouldBe
          List(Some("Value: NaN does not meet the constraint requirement!"))
        assert(colMismatchSourceResult.status == CheckStatus.Error)

        val colRenamedResult = verificationResult.checkResults(dataSyncCheckColRenamed)
        colRenamedResult.constraintResults.map(_.message) shouldBe
          List(None)
        assert(colRenamedResult.status == CheckStatus.Success)

        val fullMatchResult = verificationResult.checkResults(dataSyncFullMatch)
        fullMatchResult.constraintResults.map(_.message) shouldBe
          List(None)
        assert(fullMatchResult.status == CheckStatus.Success)

        // Assertions for the new checks
        val matchColumnsResult = verificationResult.checkResults(dataSyncCheckWithMatchColumns)
        matchColumnsResult.constraintResults.map(_.message) shouldBe
          List(None) // or any expected result
        assert(matchColumnsResult.status == CheckStatus.Success) // or expected status

        val additionalColsResult = verificationResult.checkResults(dataSyncCheckWithAdditionalCols)
        additionalColsResult.constraintResults.map(_.message) shouldBe
          List(None) // or any expected result
        assert(additionalColsResult.status == CheckStatus.Success) // or expected status

    }

    "pass verification when ColumnValues rule contains a single quote in the string" in withSparkSession { session =>
      import session.implicits._

      val df = Seq(
        ("Versicolor"),
        ("Virginica's"),
        ("Setosa"),
        ("Versicolor"),
        ("Virginica's")
      ).toDF("variety")

      val check = Check(CheckLevel.Error, "single quote check")
        .isContainedIn("variety", Array("Versicolor", "Virginica's", "Setosa"))

      val verificationResult = VerificationSuite()
        .onData(df)
        .addCheck(check)
        .run()

      assert(verificationResult.status == CheckStatus.Success)

      val checkResult = verificationResult.checkResults(check)

      assert(checkResult.status == CheckStatus.Success)

      assert(checkResult.constraintResults.size == 1)

      val constraintResult = checkResult.constraintResults.head

      assert(constraintResult.status == ConstraintStatus.Success)

      val metric = constraintResult.metric.getOrElse(fail("Expected metric to be present"))
      assert(metric.isInstanceOf[DoubleMetric])
      assert(metric.asInstanceOf[DoubleMetric].value.isSuccess)

      val metricValue = metric.asInstanceOf[DoubleMetric].value.get
      assert(metricValue == 1.0)
    }
  }

  "Verification Suite with == based Min/Max checks and filtered row behavior" should {
    val col1 = "att1"
    val col2 = "att2"
    val col3 = "att3"

    val check1Description = "equality-check-1"
    val check2Description = "equality-check-2"
    val check3Description = "equality-check-3"

    val check1WhereClause = "att1 > 3"
    val check2WhereClause = "att2 > 4"
    val check3WhereClause = "att3 = 0"

    def mkEqualityCheck1(analyzerOptions: AnalyzerOptions): Check = new Check(CheckLevel.Error, check1Description)
      .hasMin(col1, _ == 4, analyzerOptions = Some(analyzerOptions)).where(check1WhereClause)
      .hasMax(col1, _ == 4, analyzerOptions = Some(analyzerOptions)).where(check1WhereClause)

    def mkEqualityCheck2(analyzerOptions: AnalyzerOptions): Check = new Check(CheckLevel.Error, check2Description)
      .hasMin(col2, _ == 7, analyzerOptions = Some(analyzerOptions)).where(check2WhereClause)
      .hasMax(col2, _ == 7, analyzerOptions = Some(analyzerOptions)).where(check2WhereClause)

    def mkEqualityCheck3(analyzerOptions: AnalyzerOptions): Check = new Check(CheckLevel.Error, check3Description)
      .hasMin(col3, _ == 0, analyzerOptions = Some(analyzerOptions)).where(check3WhereClause)
      .hasMax(col3, _ == 0, analyzerOptions = Some(analyzerOptions)).where(check3WhereClause)

    def getRowLevelResults(df: DataFrame): Seq[java.lang.Boolean] =
      df.collect().map { r => r.getAs[java.lang.Boolean](0) }.toSeq

    def assertCheckResults(verificationResult: VerificationResult): Unit = {
      val passResult = verificationResult.checkResults

      val equalityCheck1Result = passResult.values.find(_.check.description == check1Description)
      val equalityCheck2Result = passResult.values.find(_.check.description == check2Description)
      val equalityCheck3Result = passResult.values.find(_.check.description == check3Description)

      assert(equalityCheck1Result.isDefined && equalityCheck1Result.get.status == CheckStatus.Error)
      assert(equalityCheck2Result.isDefined && equalityCheck2Result.get.status == CheckStatus.Error)
      assert(equalityCheck3Result.isDefined && equalityCheck3Result.get.status == CheckStatus.Success)
    }

    def assertRowLevelResults(rowLevelResults: DataFrame,
                              analyzerOptions: AnalyzerOptions): Unit = {
      val equalityCheck1Results = getRowLevelResults(rowLevelResults.select(check1Description))
      val equalityCheck2Results = getRowLevelResults(rowLevelResults.select(check2Description))
      val equalityCheck3Results = getRowLevelResults(rowLevelResults.select(check3Description))

      val filteredOutcome: java.lang.Boolean = analyzerOptions.filteredRow match {
        case FilteredRowOutcome.TRUE => true
        case FilteredRowOutcome.NULL => null
      }

      assert(equalityCheck1Results == Seq(filteredOutcome, filteredOutcome, filteredOutcome, true, false, false))
      assert(equalityCheck2Results == Seq(filteredOutcome, filteredOutcome, filteredOutcome, false, false, true))
      assert(equalityCheck3Results == Seq(true, true, true, filteredOutcome, filteredOutcome, filteredOutcome))
    }

    def assertMetrics(metricsDF: DataFrame): Unit = {
      val metricsMap = getMetricsAsMap(metricsDF)
      assert(metricsMap(s"$col1|Minimum (where: $check1WhereClause)") == 4.0)
      assert(metricsMap(s"$col1|Maximum (where: $check1WhereClause)") == 6.0)
      assert(metricsMap(s"$col2|Minimum (where: $check2WhereClause)") == 5.0)
      assert(metricsMap(s"$col2|Maximum (where: $check2WhereClause)") == 7.0)
      assert(metricsMap(s"$col3|Minimum (where: $check3WhereClause)") == 0.0)
      assert(metricsMap(s"$col3|Maximum (where: $check3WhereClause)") == 0.0)
    }

    "mark filtered rows as null" in withSparkSession {
      sparkSession =>
        val df = getDfWithNumericValues(sparkSession)
        val analyzerOptions = AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)

        val equalityCheck1 = mkEqualityCheck1(analyzerOptions)
        val equalityCheck2 = mkEqualityCheck2(analyzerOptions)
        val equalityCheck3 = mkEqualityCheck3(analyzerOptions)

        val verificationResult = VerificationSuite()
          .onData(df)
          .addChecks(Seq(equalityCheck1, equalityCheck2, equalityCheck3))
          .run()

        val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
        val metricsDF = VerificationResult.successMetricsAsDataFrame(sparkSession, verificationResult)

        assertCheckResults(verificationResult)
        assertRowLevelResults(rowLevelResultsDF, analyzerOptions)
        assertMetrics(metricsDF)
    }

    "mark filtered rows as true" in withSparkSession {
      sparkSession =>
        val df = getDfWithNumericValues(sparkSession)
        val analyzerOptions = AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE)

        val equalityCheck1 = mkEqualityCheck1(analyzerOptions)
        val equalityCheck2 = mkEqualityCheck2(analyzerOptions)
        val equalityCheck3 = mkEqualityCheck3(analyzerOptions)

        val verificationResult = VerificationSuite()
          .onData(df)
          .addChecks(Seq(equalityCheck1, equalityCheck2, equalityCheck3))
          .run()

        val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
        val metricsDF = VerificationResult.successMetricsAsDataFrame(sparkSession, verificationResult)

        assertCheckResults(verificationResult)
        assertRowLevelResults(rowLevelResultsDF, analyzerOptions)
        assertMetrics(metricsDF)
    }
  }

  "Verification Suite with == based Min/Max checks and null row behavior" should {
    val col = "attNull"
    val checkDescription = "equality-check"
    def mkEqualityCheck(analyzerOptions: AnalyzerOptions): Check = new Check(CheckLevel.Error, checkDescription)
      .hasMin(col, _ == 6, analyzerOptions = Some(analyzerOptions))
      .hasMax(col, _ == 6, analyzerOptions = Some(analyzerOptions))

    def assertCheckResults(verificationResult: VerificationResult, checkStatus: CheckStatus.Value): Unit = {
      val passResult = verificationResult.checkResults
      val equalityCheckResult = passResult.values.find(_.check.description == checkDescription)
      assert(equalityCheckResult.isDefined && equalityCheckResult.get.status == checkStatus)
    }

    def getRowLevelResults(df: DataFrame): Seq[java.lang.Boolean] =
      df.collect().map { r => r.getAs[java.lang.Boolean](0) }.toSeq

    def assertRowLevelResults(rowLevelResults: DataFrame,
                              analyzerOptions: AnalyzerOptions): Unit = {
      val equalityCheckResults = getRowLevelResults(rowLevelResults.select(checkDescription))
      val nullOutcome: java.lang.Boolean = analyzerOptions.nullBehavior match {
        case NullBehavior.Fail => false
        case NullBehavior.Ignore => null
      }

      assert(equalityCheckResults == Seq(nullOutcome, nullOutcome, nullOutcome, false, true, false))
    }

    def assertMetrics(metricsDF: DataFrame): Unit = {
      val metricsMap = getMetricsAsMap(metricsDF)
      assert(metricsMap(s"$col|Minimum") == 5.0)
      assert(metricsMap(s"$col|Maximum") == 7.0)
    }

    "keep non-filtered null rows as null" in withSparkSession {
      sparkSession =>
        val df = getDfWithNumericValues(sparkSession)
        val analyzerOptions = AnalyzerOptions(nullBehavior = NullBehavior.Ignore)
        val verificationResult = VerificationSuite()
          .onData(df)
          .addChecks(Seq(mkEqualityCheck(analyzerOptions)))
          .run()

        assertCheckResults(verificationResult, CheckStatus.Error)

        val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
        assertRowLevelResults(rowLevelResultsDF, analyzerOptions)

        val metricsDF = VerificationResult.successMetricsAsDataFrame(sparkSession, verificationResult)
        assertMetrics(metricsDF)
    }

    "mark non-filtered null rows as false" in withSparkSession {
      sparkSession =>
        val df = getDfWithNumericValues(sparkSession)
        val analyzerOptions = AnalyzerOptions(nullBehavior = NullBehavior.Fail)
        val verificationResult = VerificationSuite()
          .onData(df)
          .addChecks(Seq(mkEqualityCheck(analyzerOptions)))
          .run()

        assertCheckResults(verificationResult, CheckStatus.Error)

        val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
        assertRowLevelResults(rowLevelResultsDF, analyzerOptions)

        val metricsDF = VerificationResult.successMetricsAsDataFrame(sparkSession, verificationResult)
        assertMetrics(metricsDF)
    }
  }

  "Verification Suite with ==/!= based MinLength/MaxLength checks and filtered row behavior" should {
    val col1 = "Company"
    val col2 = "ZipCode"
    val col3 = "City"

    val check1Description = "length-equality-check-1"
    val check2Description = "length-equality-check-2"
    val check3Description = "length-equality-check-3"

    val check1WhereClause = "ID > 2"
    val check2WhereClause = "ID in (1, 2, 3)"
    val check3WhereClause = "ID <= 2"

    def mkLengthCheck1(analyzerOptions: AnalyzerOptions): Check = new Check(CheckLevel.Error, check1Description)
      .hasMinLength(col1, _ == 8, analyzerOptions = Some(analyzerOptions)).where(check1WhereClause)
      .hasMaxLength(col1, _ == 8, analyzerOptions = Some(analyzerOptions)).where(check1WhereClause)

    def mkLengthCheck2(analyzerOptions: AnalyzerOptions): Check = new Check(CheckLevel.Error, check2Description)
      .hasMinLength(col2, _ == 4, analyzerOptions = Some(analyzerOptions)).where(check2WhereClause)
      .hasMaxLength(col2, _ == 4, analyzerOptions = Some(analyzerOptions)).where(check2WhereClause)

    def mkLengthCheck3(analyzerOptions: AnalyzerOptions): Check = new Check(CheckLevel.Error, check3Description)
      .hasMinLength(col3, _ != 0, analyzerOptions = Some(analyzerOptions)).where(check3WhereClause)
      .hasMaxLength(col3, _ != 0, analyzerOptions = Some(analyzerOptions)).where(check3WhereClause)

    def getRowLevelResults(df: DataFrame): Seq[java.lang.Boolean] =
      df.collect().map { r => r.getAs[java.lang.Boolean](0) }.toSeq

    def assertCheckResults(verificationResult: VerificationResult): Unit = {
      val passResult = verificationResult.checkResults

      val equalityCheck1Result = passResult.values.find(_.check.description == check1Description)
      val equalityCheck2Result = passResult.values.find(_.check.description == check2Description)
      val equalityCheck3Result = passResult.values.find(_.check.description == check3Description)

      assert(equalityCheck1Result.isDefined && equalityCheck1Result.get.status == CheckStatus.Success)
      assert(equalityCheck2Result.isDefined && equalityCheck2Result.get.status == CheckStatus.Error)
      assert(equalityCheck3Result.isDefined && equalityCheck3Result.get.status == CheckStatus.Success)
    }

    def assertRowLevelResults(rowLevelResults: DataFrame,
                              analyzerOptions: AnalyzerOptions): Unit = {
      val equalityCheck1Results = getRowLevelResults(rowLevelResults.select(check1Description))
      val equalityCheck2Results = getRowLevelResults(rowLevelResults.select(check2Description))
      val equalityCheck3Results = getRowLevelResults(rowLevelResults.select(check3Description))

      val filteredOutcome: java.lang.Boolean = analyzerOptions.filteredRow match {
        case FilteredRowOutcome.TRUE => true
        case FilteredRowOutcome.NULL => null
      }

      assert(equalityCheck1Results == Seq(filteredOutcome, filteredOutcome, true, true))
      assert(equalityCheck2Results == Seq(false, false, false, filteredOutcome))
      assert(equalityCheck3Results == Seq(true, true, filteredOutcome, filteredOutcome))
    }

    def assertMetrics(metricsDF: DataFrame): Unit = {
      val metricsMap = getMetricsAsMap(metricsDF)
      assert(metricsMap(s"$col1|MinLength (where: $check1WhereClause)") == 8.0)
      assert(metricsMap(s"$col1|MaxLength (where: $check1WhereClause)") == 8.0)
      assert(metricsMap(s"$col2|MinLength (where: $check2WhereClause)") == 0.0)
      assert(metricsMap(s"$col2|MaxLength (where: $check2WhereClause)") == 5.0)
      assert(metricsMap(s"$col3|MinLength (where: $check3WhereClause)") == 11.0)
      assert(metricsMap(s"$col3|MaxLength (where: $check3WhereClause)") == 11.0)
    }

    "mark filtered rows as null" in withSparkSession {
      sparkSession =>
        val df = getDfForWhereClause(sparkSession)
        val analyzerOptions = AnalyzerOptions(
          nullBehavior = NullBehavior.EmptyString, filteredRow = FilteredRowOutcome.NULL
        )

        val equalityCheck1 = mkLengthCheck1(analyzerOptions)
        val equalityCheck2 = mkLengthCheck2(analyzerOptions)
        val equalityCheck3 = mkLengthCheck3(analyzerOptions)

        val verificationResult = VerificationSuite()
          .onData(df)
          .addChecks(Seq(equalityCheck1, equalityCheck2, equalityCheck3))
          .run()

        val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
        val metricsDF = VerificationResult.successMetricsAsDataFrame(sparkSession, verificationResult)

        assertCheckResults(verificationResult)
        assertRowLevelResults(rowLevelResultsDF, analyzerOptions)
        assertMetrics(metricsDF)
    }

    "mark filtered rows as true" in withSparkSession {
      sparkSession =>
        val df = getDfForWhereClause(sparkSession)
        val analyzerOptions = AnalyzerOptions(
          nullBehavior = NullBehavior.EmptyString, filteredRow = FilteredRowOutcome.TRUE
        )

        val equalityCheck1 = mkLengthCheck1(analyzerOptions)
        val equalityCheck2 = mkLengthCheck2(analyzerOptions)
        val equalityCheck3 = mkLengthCheck3(analyzerOptions)

        val verificationResult = VerificationSuite()
          .onData(df)
          .addChecks(Seq(equalityCheck1, equalityCheck2, equalityCheck3))
          .run()

        val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
        val metricsDF = VerificationResult.successMetricsAsDataFrame(sparkSession, verificationResult)

        assertCheckResults(verificationResult)
        assertRowLevelResults(rowLevelResultsDF, analyzerOptions)
        assertMetrics(metricsDF)
    }
  }

  "Verification Suite with ==/!= based MinLength/MaxLength checks and null row behavior" should {
    val col = "City"
    val checkDescription = "length-check"
    val assertion = (d: Double) => d >= 0.0 && d <= 8.0

    def mkLengthCheck(analyzerOptions: AnalyzerOptions): Check = new Check(CheckLevel.Error, checkDescription)
      .hasMinLength(col, assertion, analyzerOptions = Some(analyzerOptions))
      .hasMaxLength(col, assertion, analyzerOptions = Some(analyzerOptions))

    def assertCheckResults(verificationResult: VerificationResult, checkStatus: CheckStatus.Value): Unit = {
      val passResult = verificationResult.checkResults
      val equalityCheckResult = passResult.values.find(_.check.description == checkDescription)
      assert(equalityCheckResult.isDefined && equalityCheckResult.get.status == checkStatus)
    }

    def getRowLevelResults(df: DataFrame): Seq[java.lang.Boolean] =
      df.collect().map { r => r.getAs[java.lang.Boolean](0) }.toSeq

    def assertRowLevelResults(rowLevelResults: DataFrame,
                              analyzerOptions: AnalyzerOptions): Unit = {
      val equalityCheckResults = getRowLevelResults(rowLevelResults.select(checkDescription))
      val nullOutcome: java.lang.Boolean = analyzerOptions.nullBehavior match {
        case NullBehavior.Fail => false
        case NullBehavior.Ignore => null
        case NullBehavior.EmptyString => true
      }

      assert(equalityCheckResults == Seq(false, false, nullOutcome, true))
    }

    def assertMetrics(metricsDF: DataFrame, minLength: Double, maxLength: Double): Unit = {
      val metricsMap = getMetricsAsMap(metricsDF)
      assert(metricsMap(s"$col|MinLength") == minLength)
      assert(metricsMap(s"$col|MaxLength") == maxLength)
    }

    "keep non-filtered null rows as null" in withSparkSession {
      sparkSession =>
        val df = getDfForWhereClause(sparkSession)
        val analyzerOptions = AnalyzerOptions(nullBehavior = NullBehavior.Ignore)
        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(mkLengthCheck(analyzerOptions))
          .run()

        assertCheckResults(verificationResult, CheckStatus.Error)

        val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
        assertRowLevelResults(rowLevelResultsDF, analyzerOptions)

        val metricsDF = VerificationResult.successMetricsAsDataFrame(sparkSession, verificationResult)
        assertMetrics(metricsDF, 8.0, 11.0)
    }

    "mark non-filtered null rows as false" in withSparkSession {
      sparkSession =>
        val df = getDfForWhereClause(sparkSession)
        val analyzerOptions = AnalyzerOptions(nullBehavior = NullBehavior.Fail)
        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(mkLengthCheck(analyzerOptions))
          .run()

        assertCheckResults(verificationResult, CheckStatus.Error)

        val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
        assertRowLevelResults(rowLevelResultsDF, analyzerOptions)

        val metricsDF = VerificationResult.successMetricsAsDataFrame(sparkSession, verificationResult)
        assertMetrics(metricsDF, Double.MinValue, Double.MaxValue)
    }

    "mark non-filtered null rows as empty string" in withSparkSession {
      sparkSession =>
        val df = getDfForWhereClause(sparkSession)
        val analyzerOptions = AnalyzerOptions(nullBehavior = NullBehavior.EmptyString)
        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(mkLengthCheck(analyzerOptions))
          .run()

        assertCheckResults(verificationResult, CheckStatus.Error)

        val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
        assertRowLevelResults(rowLevelResultsDF, analyzerOptions)

        val metricsDF = VerificationResult.successMetricsAsDataFrame(sparkSession, verificationResult)
        assertMetrics(metricsDF, 0.0, 11.0)
    }
  }

  "Verification Suite's Row Level Results" should {
    "yield correct results for invalid column type" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        ("1", 1, "blue"),
        ("2", 2, "green"),
        ("3", 3, "blue"),
        ("4", 4, "red"),
        ("5", 5, "purple")
      ).toDF("id", "id2", "color")

      val idColumn = "id"
      val id2Column = "id2"

      val minCheckOnInvalidColumnDescription = s"min check on $idColumn"
      val minCheckOnValidColumnDescription = s"min check on $id2Column"
      val patternMatchCheckOnInvalidColumnDescription = s"pattern check on $id2Column"
      val patternMatchCheckOnValidColumnDescription = s"pattern check on $idColumn"

      val minCheckOnInvalidColumn = Check(CheckLevel.Error, minCheckOnInvalidColumnDescription)
        .hasMin(idColumn, _ >= 3)
        .isComplete(idColumn)
      val minCheckOnValidColumn = Check(CheckLevel.Error, minCheckOnValidColumnDescription)
        .hasMin(id2Column, _ >= 3)
        .isComplete(id2Column)

      val patternMatchCheckOnInvalidColumn = Check(CheckLevel.Error, patternMatchCheckOnInvalidColumnDescription)
        .hasPattern(id2Column, "[0-3]+".r)
      val patternMatchCheckOnValidColumn = Check(CheckLevel.Error, patternMatchCheckOnValidColumnDescription)
        .hasPattern(idColumn, "[0-3]+".r)

      val checks = Seq(
        minCheckOnInvalidColumn,
        minCheckOnValidColumn,
        patternMatchCheckOnInvalidColumn,
        patternMatchCheckOnValidColumn
      )

      val verificationResult = VerificationSuite().onData(df).addChecks(checks).run()
      val rowLevelResultsDF = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
      val rowLevelResults = rowLevelResultsDF.collect()

      val minCheckOnInvalidColumnRowLevelResults =
        rowLevelResults.map(_.getAs[Boolean](minCheckOnInvalidColumnDescription))
      val minCheckOnValidColumnRowLevelResults =
        rowLevelResults.map(_.getAs[Boolean](minCheckOnValidColumnDescription))
      val patternMatchCheckOnInvalidColumnRowLevelResults =
        rowLevelResults.map(_.getAs[Boolean](patternMatchCheckOnInvalidColumnDescription))
      val patternMatchCheckOnValidColumnRowLevelResults =
        rowLevelResults.map(_.getAs[Boolean](patternMatchCheckOnValidColumnDescription))

      minCheckOnInvalidColumnRowLevelResults shouldBe Seq(false, false, false, false, false)
      minCheckOnValidColumnRowLevelResults shouldBe Seq(false, false, true, true, true)
      patternMatchCheckOnInvalidColumnRowLevelResults shouldBe Seq(false, false, false, false, false)
      patternMatchCheckOnValidColumnRowLevelResults shouldBe Seq(true, true, true, false, false)
    }

    "yield correct results for satisfies check" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        (1, "blue"),
        (2, "green"),
        (3, "blue"),
        (4, "red"),
        (5, "purple")
      ).toDF("id", "color")

      val columnCondition = "color in ('blue')"
      val whereClause = "id <= 3"

      case class CheckConfig(checkName: String,
                             assertion: Double => Boolean,
                             checkStatus: CheckStatus.Value,
                             whereClause: Option[String] = None)

      val success = CheckStatus.Success
      val error = CheckStatus.Error

      val checkConfigs = Seq(
        // Without where clause: Expected compliance metric for full dataset for given condition is 0.4
        CheckConfig("check with >", (d: Double) => d > 0.5, error),
        CheckConfig("check with >=", (d: Double) => d >= 0.35, success),
        CheckConfig("check with <", (d: Double) => d < 0.3, error),
        CheckConfig("check with <=", (d: Double) => d <= 0.4, success),
        CheckConfig("check with =", (d: Double) => d == 0.4, success),
        CheckConfig("check with > / <", (d: Double) => d > 0.0 && d < 0.5, success),
        CheckConfig("check with >= / <=", (d: Double) => d >= 0.41 && d <= 1.1, error),

        // With where Clause: Expected compliance metric for full dataset for given condition with where clause is 0.67
        CheckConfig("check w/ where and with >", (d: Double) => d > 0.7, error, Some(whereClause)),
        CheckConfig("check w/ where and with >=", (d: Double) => d >= 0.66, success, Some(whereClause)),
        CheckConfig("check w/ where and with <", (d: Double) => d < 0.6, error, Some(whereClause)),
        CheckConfig("check w/ where and with <=", (d: Double) => d <= 0.67, success, Some(whereClause)),
        CheckConfig("check w/ where and with =", (d: Double) => d == 0.66, error, Some(whereClause)),
        CheckConfig("check w/ where and with > / <", (d: Double) => d > 0.0 && d < 0.5, error, Some(whereClause)),
        CheckConfig("check w/ where and with >= / <=", (d: Double) => d >= 0.41 && d <= 1.1, success, Some(whereClause))
      )

      val checks = checkConfigs.map { checkConfig =>
        val constraintName = s"Constraint for check: ${checkConfig.checkName}"
        val check = Check(CheckLevel.Error, checkConfig.checkName)
          .satisfies(columnCondition, constraintName, checkConfig.assertion)
        checkConfig.whereClause.map(check.where).getOrElse(check)
      }

      val verificationResult = VerificationSuite().onData(df).addChecks(checks).run()
      val actualResults = verificationResult.checkResults.map { case (c, r) => c.description -> r.status }
      val expectedResults = checkConfigs.map { c => c.checkName -> c.checkStatus}.toMap
      assert(actualResults == expectedResults)

      verificationResult.metrics.values.foreach { metric =>
        val metricValue = metric.asInstanceOf[Metric[Double]].value.toOption.getOrElse(0.0)
        if (metric.instance.contains("where")) assert(math.abs(metricValue - 0.66) < 0.1)
        else assert(metricValue == 0.4)
      }

      val rowLevelResults = VerificationResult.rowLevelResultsAsDataFrame(sparkSession, verificationResult, df)
      checkConfigs.foreach { checkConfig =>
        val results = rowLevelResults.select(checkConfig.checkName).collect().map { r => r.getAs[Boolean](0)}.toSeq
        if (checkConfig.whereClause.isDefined) assert(results == Seq(true, false, true, true, true))
        else assert(results == Seq(true, false, true, false, false))
      }
    }
  }

   /** Run anomaly detection using a repository with some previous analysis results for testing */
  private[this] def evaluateWithRepositoryWithHistory(test: MetricsRepository => Unit): Unit = {

    val repository = new InMemoryMetricsRepository()

    (1 to 2).foreach { timeStamp =>
      val analyzerContext = new AnalyzerContext(Map(
        Size() -> DoubleMetric(Entity.Column, "", "", util.Success(timeStamp))
      ))
      repository.save(ResultKey(timeStamp, Map("Region" -> "EU")), analyzerContext)
    }

    (3 to 4).foreach { timeStamp =>
      val analyzerContext = new AnalyzerContext(Map(
        Size() -> DoubleMetric(Entity.Column, "", "", util.Success(timeStamp))
      ))
      repository.save(ResultKey(timeStamp, Map("Region" -> "NA")), analyzerContext)
    }
    test(repository)
  }

  private[this] def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit = {
    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)
  }

  private[this] def getMetricsAsMap(metricsDF: DataFrame): Map[String, Double] = {
    metricsDF.collect().map { r =>
      val colName = r.getAs[String]("instance")
      val metricName = r.getAs[String]("name")
      val metricValue = r.getAs[Double]("value")
      s"$colName|$metricName" -> metricValue
    }.toMap
  }
}
