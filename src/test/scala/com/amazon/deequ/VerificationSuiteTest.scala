/** Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.anomalydetection.AbsoluteChangeStrategy
import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.Constraint
import com.amazon.deequ.io.DfsUtils
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import com.amazon.deequ.repository.MetricsRepository
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.utils.CollectionUtils.SeqExtensions
import com.amazon.deequ.utils.FixtureSupport
import com.amazon.deequ.utils.TempFileUtils
import org.apache.spark.sql.DataFrame
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.scalatest.WordSpec

class VerificationSuiteTest
    extends WordSpec
    with Matchers
    with SparkContextSpec
    with FixtureSupport
    with MockFactory {

  "Verification Suite" should {

    "return the correct verification status regardless of the order of checks" in
      withSparkSession { sparkSession =>
        def assertStatusFor(data: DataFrame, checks: Check*)(
            expectedStatus: CheckStatus.Value
        ): Unit = {
          val verificationSuiteStatus =
            VerificationSuite().onData(data).addChecks(checks).run().status
          assert(verificationSuiteStatus == expectedStatus)
        }

        def printCheckResults(data: DataFrame, checks: Check*): Unit = {

          val result: VerificationResult =
            VerificationSuite().onData(data).addChecks(checks).run()
          println("### CHECK RESULTS ###")
          data.show(100)
          println(
            data.dtypes.map(f => println(f._1 + "," + f._2)).mkString(", ")
          )

          val json = VerificationResult.checkResultsAsJson(result, checks)

          println(s"$json")
        }

        def printSuccessMetricsAsJson(data: DataFrame, checks: Check*): Unit = {
          val results: VerificationResult =
            VerificationSuite().onData(data).addChecks(checks).run()
          println("### SUCCESS METRICS ###")
          data.show(100)
          println(
            data.dtypes.map(f => println(f._1 + "," + f._2)).mkString(", ")
          )

          val analyzers = results.metrics.keys.toSeq
          val json = VerificationResult.successMetricsAsJson(results, analyzers)

          println(s"$json")
        }

        val customerDF = sparkSession.read
          .format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load("test-data/customer.csv")

        val df = getDfCompleteAndInCompleteColumns(sparkSession)
        val dfInteger = getDfCompleteAndInCompleteColumnsInteger(sparkSession)

        val checkToSucceedInteger = Check(CheckLevel.Error, "group-1")
          .isComplete("integer1")
          .hasCompleteness("string1", _ == 1.0)

        val checkToSucceedIntegerCustomer = Check(CheckLevel.Error, "group-2")
          .isComplete("Customer_ID")
          .isComplete("Address_1")
          .isComplete("Zip")
          .hasCompleteness("Customer_ID", _ == 1.0)
          .hasCompleteness("Address_1", _ == 1.0)
          .hasCompleteness("Zip", _ == 1.0)

        val checkToSucceed = Check(CheckLevel.Error, "group-1")
          .isComplete("att1")
          .hasCompleteness("att1", _ == 1.0)

        val checkToErrorOut = Check(CheckLevel.Error, "group-2-E")
          .hasCompleteness("att2", _ > 0.8)

        val checkToWarn = Check(CheckLevel.Warning, "group-2-W")
          .hasCompleteness("item", _ < 0.8)

        println("ASSERT INTEGER COLUMN COMPLETENESS")
        printSuccessMetricsAsJson(customerDF, checkToSucceedIntegerCustomer)
        printCheckResults(customerDF, checkToSucceedIntegerCustomer)
        printSuccessMetricsAsJson(dfInteger, checkToSucceedInteger)
        printCheckResults(dfInteger, checkToSucceedInteger)
        printSuccessMetricsAsJson(df, checkToSucceed)
        printCheckResults(df, checkToSucceed)
        printSuccessMetricsAsJson(df, checkToSucceed)
        printCheckResults(df, checkToErrorOut)
        printSuccessMetricsAsJson(df, checkToErrorOut)
        printCheckResults(df, checkToWarn)
        printSuccessMetricsAsJson(df, checkToWarn)

        Seq(checkToSucceed, checkToErrorOut).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Error)
        }

        Seq(checkToSucceed, checkToWarn).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Warning)
        }

        Seq(checkToWarn, checkToErrorOut).forEachOrder { checks =>
          assertStatusFor(df, checks: _*)(CheckStatus.Error)
        }

        Seq(checkToSucceed, checkToWarn, checkToErrorOut).forEachOrder {
          checks =>
            assertStatusFor(df, checks: _*)(CheckStatus.Error)
        }
      }

<<<<<<< HEAD
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
      val minLength = new Check(CheckLevel.Error, "rule3")
        .hasMinLength("item", _ >= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val maxLength = new Check(CheckLevel.Error, "rule4")
        .hasMaxLength("item", _ <= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val patternMatch = new Check(CheckLevel.Error, "rule6").hasPattern("att2", "[a-z]".r)
      val min = new Check(CheckLevel.Error, "rule7").hasMin("val1", _ > 1)
      val max = new Check(CheckLevel.Error, "rule8").hasMax("val1", _ <= 3)
      val compliance = new Check(CheckLevel.Error, "rule9")
        .satisfies("item < 1000", "rule9", columns = List("item"))
      val expectedColumn1 = isComplete.description
      val expectedColumn2 = completeness.description
      val expectedColumn3 = minLength.description
      val expectedColumn4 = maxLength.description
      val expectedColumn5 = patternMatch.description
      val expectedColumn6 = min.description
      val expectedColumn7 = max.description
      val expectedColumn8 = compliance.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(isComplete)
        .addCheck(completeness)
        .addCheck(minLength)
        .addCheck(maxLength)
        .addCheck(patternMatch)
        .addCheck(min)
        .addCheck(max)
        .addCheck(compliance)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)

      resultData.show()
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 + expectedColumn4 +
          expectedColumn5 + expectedColumn6 + expectedColumn7 + expectedColumn8
      assert(resultData.columns.toSet == expectedColumns)


      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel2))

      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel3))

      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.getBoolean(0))
      assert(Seq(true, false, false, false, false, false).sameElements(rowLevel4))

      val rowLevel5 = resultData.select(expectedColumn5).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel5))

      val rowLevel6 = resultData.select(expectedColumn6).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(false, true, true, true, true, true).sameElements(rowLevel6))

      val rowLevel7 = resultData.select(expectedColumn7).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(true, true, true, false, false, false).sameElements(rowLevel7))

      val rowLevel8 = resultData.select(expectedColumn8).collect().map(r => r.getAs[Boolean](0))
      assert(Seq(true, true, true, false, false, false).sameElements(rowLevel8))
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

      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r =>
        if (r == null) null else r.getAs[Boolean](0))
      assert(Seq(false, null, true, true, null, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r =>
        if (r == null) null else r.getAs[Boolean](0))
      assert(Seq(true, null, true, false, null, false).sameElements(rowLevel2))

      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r =>
        if (r == null) null else r.getAs[Boolean](0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel3))

      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r =>
        if (r == null) null else r.getAs[Boolean](0))
      assert(Seq(false, null, false, true, null, true).sameElements(rowLevel4))
    }

    "generate a result that contains length row-level results with nullBehavior fail" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(session)

      val minLength = new Check(CheckLevel.Error, "rule1")
        .hasMinLength("att2", _ >= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val maxLength = new Check(CheckLevel.Error, "rule2")
        .hasMaxLength("att2", _ <= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val expectedColumn1 = minLength.description
      val expectedColumn2 = maxLength.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(minLength)
        .addCheck(maxLength)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)

      resultData.show()
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2
      assert(resultData.columns.toSet == expectedColumns)

      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel2))
    }

    "generate a result that contains length row-level results with nullBehavior empty" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(session)

      // null should fail since length 0 is not >= 1
      val minLength = new Check(CheckLevel.Error, "rule1")
        .hasMinLength("att2", _ >= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString)))
      // nulls should succeed since length 0 is < 2
      val maxLength = new Check(CheckLevel.Error, "rule2")
        .hasMaxLength("att2", _ < 2, analyzerOptions = Option(AnalyzerOptions(NullBehavior.EmptyString)))
      val expectedColumn1 = minLength.description
      val expectedColumn2 = maxLength.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(minLength)
        .addCheck(maxLength)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)

      resultData.show()
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2
      assert(resultData.columns.toSet == expectedColumns)


      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel2))
    }

    "accept analysis config for mandatory analysis" in withSparkSession { sparkSession =>
        import sparkSession.implicits._
        val df = getDfFull(sparkSession)

        val result = {
          val checkToSucceed = Check(CheckLevel.Error, "group-1")
            .isComplete("att1") // 1.0
            .hasCompleteness("att1", _ == 1.0) // 1.0

          val analyzers =
            Size() :: // Analyzer that works on overall document
              Completeness("att2") ::
              Uniqueness("att2") :: // Analyzer that works on single column
              MutualInformation(
                "att1",
                "att2"
              ) :: Nil // Analyzer that works on multi column

          VerificationSuite()
            .onData(df)
            .addCheck(checkToSucceed)
            .addRequiredAnalyzers(analyzers)
            .run()
        }

        assert(result.status == CheckStatus.Success)

        val analysisDf = AnalyzerContext.successMetricsAsDataFrame(
          sparkSession,
          AnalyzerContext(result.metrics)
        )

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

    "run the analysis even there are no constraints" in withSparkSession {
      sparkSession =>
        import sparkSession.implicits._
        val df = getDfFull(sparkSession)

        VerificationSuite().onData(df).addRequiredAnalyzer(Size()).run() match {
          case result =>
            assert(result.status == CheckStatus.Success)

            val analysisDf = AnalyzerContext.successMetricsAsDataFrame(
              sparkSession,
              AnalyzerContext(result.metrics)
            )

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

        val verificationResult = VerificationSuite()
          .onData(df)
          .addRequiredAnalyzer(analyzerToTestReusingResults)
          .run()
        val analysisResult = AnalyzerContext(verificationResult.metrics)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)
        repository.save(resultKey, analysisResult)

        val analyzers = analyzerToTestReusingResults :: Uniqueness(Seq("att2", "item")) :: Nil

        val (separateResults, numSeparateJobs) =
          sparkMonitor.withMonitoringSession { stat =>
            val results = analyzers.map { _.calculate(df) }.toSet
            (results, stat.jobCount)
          }

        val (runnerResults, numCombinedJobs) =
          sparkMonitor.withMonitoringSession { stat =>
            val results = VerificationSuite()
              .onData(df)
              .useRepository(repository)
              .reuseExistingResultsForKey(resultKey)
              .addRequiredAnalyzers(analyzers)
              .run()
              .metrics
              .values
              .toSet

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

        val metrics = VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .saveOrAppendResult(resultKey)
          .run()
          .metrics

        val analyzerContext = AnalyzerContext(metrics)

        assert(analyzerContext == repository.loadByKey(resultKey).get)
      }

    "only append results to repository without unnecessarily overwriting existing ones" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        val completeMetricResults = VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .saveOrAppendResult(resultKey)
          .run()
          .metrics

        val completeAnalyzerContext = AnalyzerContext(completeMetricResults)

        // Calculate and save results for first analyzer
        VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzer(Size())
          .saveOrAppendResult(resultKey)
          .run()

        // Calculate and append results for second analyzer
        VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzer(Completeness("item"))
          .saveOrAppendResult(resultKey)
          .run()

        assert(completeAnalyzerContext == repository.loadByKey(resultKey).get)
      }

    "if there are previous results in the repository new results should pre preferred in case of " +
      "conflicts" in withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        val actualResult = VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .run()
        val expectedAnalyzerContextOnLoadByKey =
          AnalyzerContext(actualResult.metrics)

        val resultWhichShouldBeOverwritten = AnalyzerContext(
          Map(Size() -> DoubleMetric(Entity.Dataset, "", "", util.Try(100.0)))
        )

        repository.save(resultKey, resultWhichShouldBeOverwritten)

        // This should overwrite the previous Size value
        VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .saveOrAppendResult(resultKey)
          .run()

        assert(
          expectedAnalyzerContextOnLoadByKey == repository
            .loadByKey(resultKey)
            .get
        )
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
            Some(
              AnomalyCheckConfig(CheckLevel.Warning, "Anomaly check to fail")
            )
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
            Some(
              AnomalyCheckConfig(
                CheckLevel.Error,
                "Anomaly check to succeed",
                Map.empty,
                Some(0),
                Some(11)
              )
            )
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
        val checkResultsThree =
          verificationResultThree.checkResults.head._2.status

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
              Some(
                AnomalyCheckConfig(CheckLevel.Warning, "Anomaly check to fail")
              )
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
              Some(
                AnomalyCheckConfig(
                  CheckLevel.Error,
                  "Anomaly check to succeed",
                  Map.empty,
                  Some(0),
                  Some(11)
                )
              )
            )
            .run()

          val checkResultsOne =
            verificationResultOne.checkResults.values.toSeq(1).status
          val checkResultsTwo =
            verificationResultTwo.checkResults.head._2.status

          assert(checkResultsOne == CheckStatus.Warning)
          assert(checkResultsTwo == CheckStatus.Success)
        }
      }

    "write output files to specified locations" in withSparkSession {
      sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val checkToSucceed = Check(CheckLevel.Error, "group-1")
          .isComplete("att1") // 1.0
          .hasCompleteness("att1", _ == 1.0) // 1.0

        val tempDir = TempFileUtils.tempDir("verificationOuput")
        val checkResultsPath = tempDir + "/check-result.json"
        val successMetricsPath = tempDir + "/success-metrics.json"

        VerificationSuite()
          .onData(df)
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

      analyzers.zip(states).foreach {
        case (analyzer: Analyzer[_, _], state: State[_]) =>
          (statePersister.persist[state.type] _)
            .expects(
              where { (analyzer_, state_) =>
                analyzer_ == analyzer && state_ == state
              }
            )
            .returns()
            .atLeastOnce()
      }

      VerificationSuite()
        .onData(df)
        .addRequiredAnalyzers(analyzers)
        .saveStatesWith(statePersister)
        .run()
    }

    "load stored states for aggregation if specified" in withSparkSession {
      sparkSession =>
        val stateLoaderStub = stub[StateLoader]

        val df = getDfWithNumericValues(sparkSession)
        val analyzers = Sum("att2") :: Completeness("att1") :: Nil

        (stateLoaderStub.load[SumState] _)
          .when(Sum("att2"))
          .returns(Some(SumState(18.0)))

        (stateLoaderStub.load[NumMatchesAndCount] _)
          .when(Completeness("att1"))
          .returns(Some(NumMatchesAndCount(0, 6)))

        val results = VerificationSuite()
          .onData(df)
          .addRequiredAnalyzers(analyzers)
          .aggregateWith(stateLoaderStub)
          .run()

        assert(results.metrics(Sum("att2")).value.get == 18.0 * 2)
        assert(results.metrics(Completeness("att1")).value.get == 0.5)
    }

    "keep order of check constraints and their results" in withSparkSession {
      sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val expectedConstraints = Seq(
          Constraint.completenessConstraint("att1", _ == 1.0),
          Constraint.complianceConstraint("att1 is positive", "att1", _ == 1.0)
        )

        val check =
          expectedConstraints.foldLeft(Check(CheckLevel.Error, "check")) {
            case (currentCheck, constraint) =>
              currentCheck.addConstraint(constraint)
          }

        // constraints are added in expected order to Check object
        assert(check.constraints == expectedConstraints)
        assert(check.constraints != expectedConstraints.reverse)

        val results = VerificationSuite()
          .onData(df)
          .addCheck(check)
          .run()

        val checkConstraintsWithResultConstraints =
          check.constraints.zip(results.checkResults(check).constraintResults)

        checkConstraintsWithResultConstraints.foreach {
          case (checkConstraint, checkResultConstraint) =>
            assert(checkConstraint == checkResultConstraint.constraint)
        }
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

        val verificationResult = VerificationSuite()
          .onData(df)
          .addCheck(checkThatShouldSucceed)
          .addCheck(isCompleteCheckThatShouldFailCompleteness)
          .addCheck(complianceCheckThatShouldSucceed)
          .addCheck(complianceCheckThatShouldFailForAge)
          .addCheck(checkThatShouldFail)
          .addCheck(complianceCheckThatShouldFail)
          .addCheck(complianceCheckThatShouldFailCompleteness)
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
            "None,List(name)), all input values were NULL."))
        assert(checkFailedResultStringType.status == CheckStatus.Error)

        val checkFailedCompletenessResult = verificationResult.checkResults(complianceCheckThatShouldFailCompleteness)
        checkFailedCompletenessResult.constraintResults.map(_.message) shouldBe
          List(Some("Input data does not include column fake!"))
        assert(checkFailedCompletenessResult.status == CheckStatus.Error)
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
  }

  /** Run anomaly detection using a repository with some previous analysis
    * results for testing
    */
  private[this] def evaluateWithRepositoryWithHistory(
      test: MetricsRepository => Unit
  ): Unit = {

    val repository = new InMemoryMetricsRepository()

    (1 to 2).foreach { timeStamp =>
      val analyzerContext = new AnalyzerContext(
        Map(
          Size() -> DoubleMetric(Entity.Column, "", "", util.Success(timeStamp))
        )
      )
      repository.save(
        ResultKey(timeStamp, Map("Region" -> "EU")),
        analyzerContext
      )
    }

    (3 to 4).foreach { timeStamp =>
      val analyzerContext = new AnalyzerContext(
        Map(
          Size() -> DoubleMetric(Entity.Column, "", "", util.Success(timeStamp))
        )
      )
      repository.save(
        ResultKey(timeStamp, Map("Region" -> "NA")),
        analyzerContext
      )
    }
    test(repository)
  }

  private[this] def assertSameRows(
      dataframeA: DataFrame,
      dataframeB: DataFrame
  ): Unit = {
    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)
  }
}
