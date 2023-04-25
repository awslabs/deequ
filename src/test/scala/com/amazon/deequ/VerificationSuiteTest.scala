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

      val overlapUniqueness = new Check(CheckLevel.Error, "rule1").hasUniqueness(Seq("nonUnique", "halfUniqueCombinedWithNonUnique"), Check.IsOne)
      val hasFullUniqueness = new Check(CheckLevel.Error, "rule2").hasUniqueness(Seq("nonUnique", "onlyUniqueWithOtherNonUnique"), Check.IsOne)
      val uniquenessWithNulls = new Check(CheckLevel.Error, "rule3").hasUniqueness(Seq("unique", "nonUniqueWithNulls"), Check.IsOne)
      val unique = new Check(CheckLevel.Error, "rule4").isUnique("unique")
      val nonUnique = new Check(CheckLevel.Error, "rule5").isUnique("nonUnique")
      val nullPrimaryKey = new Check(CheckLevel.Error, "rule6").isPrimaryKey("uniqueWithNulls")

      val expectedColumn1 = overlapUniqueness.description
      val expectedColumn2 = hasFullUniqueness.description
      val expectedColumn3 = uniquenessWithNulls.description
      val expectedColumn4 = unique.description
      val expectedColumn5 = nonUnique.description
      val expectedColumn6 = nullPrimaryKey.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(overlapUniqueness)
        .addCheck(hasFullUniqueness)
        .addCheck(uniquenessWithNulls)
        .addCheck(unique)
        .addCheck(nonUnique)
        .addCheck(nullPrimaryKey)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)
      resultData.show()

      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 + expectedColumn4 + expectedColumn5 + expectedColumn6
      assert(resultData.columns.toSet == expectedColumns)

      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.get(0))
      assert(Seq(false, false, false, true, true, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.get(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel2))

      val rowLevel3 = resultData.orderBy("unique").select(expectedColumn3).collect().map(r => r.get(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel3))

      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.get(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel4))

      val rowLevel5 = resultData.select(expectedColumn5).collect().map(r => r.get(0))
      assert(Seq(false, false, false, true, true, true).sameElements(rowLevel5))

      // TODO: fix how primaryKey works (nulls should be false)
      val rowLevel6 = resultData.orderBy("unique").select(expectedColumn6).collect().map(r => r.get(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel6))
    }

    "generate a result that contains row-level results" in withSparkSession { session =>
      val data = getDfCompleteAndInCompleteColumnsAndVarLengthStrings(session)

      val isComplete = new Check(CheckLevel.Error, "rule1").isComplete("att1")
      val completeness = new Check(CheckLevel.Error, "rule2").hasCompleteness("att2", _ > 0.7)
      val minLength = new Check(CheckLevel.Error, "rule3")
        .hasMinLength("item", _ >= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val maxLength = new Check(CheckLevel.Error, "rule4")
        .hasMaxLength("item", _ <= 1, analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val expectedColumn1 = isComplete.description
      val expectedColumn2 = completeness.description
      val expectedColumn3 = minLength.description
      val expectedColumn4 = maxLength.description

      val suite = new VerificationSuite().onData(data)
        .addCheck(isComplete)
        .addCheck(completeness)
        .addCheck(minLength)
        .addCheck(maxLength)

      val result: VerificationResult = suite.run()

      assert(result.status == CheckStatus.Error)

      val resultData = VerificationResult.rowLevelResultsAsDataFrame(session, result, data)

      resultData.show()
      val expectedColumns: Set[String] =
        data.columns.toSet + expectedColumn1 + expectedColumn2 + expectedColumn3 + expectedColumn4
      assert(resultData.columns.toSet == expectedColumns)


      val rowLevel1 = resultData.select(expectedColumn1).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel1))

      val rowLevel2 = resultData.select(expectedColumn2).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, false, true, false, true).sameElements(rowLevel2))

      val rowLevel3 = resultData.select(expectedColumn3).collect().map(r => r.getBoolean(0))
      assert(Seq(true, true, true, true, true, true).sameElements(rowLevel3))

      val rowLevel4 = resultData.select(expectedColumn4).collect().map(r => r.getBoolean(0))
      assert(Seq(true, false, false, false, false, false).sameElements(rowLevel4))
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
        ("Mutlicolumn", "att1,att2", "MutualInformation",
          -(0.75 * math.log(0.75) + 0.25 * math.log(0.25))))
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
        Constraint.complianceConstraint("att1 is positive", "att1", _ == 1.0)
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
}
