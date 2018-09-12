package com.amazon.deequ

import com.amazon.deequ.anomalydetection.RateOfChangeStrategy
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.utils.CollectionUtils.SeqExtensions
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.DataFrame
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Success, Try}


class VerificationSuiteTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "Verification Suite" should {

    "return the correct verification status regardless of the order of checks" in
      withSparkSession { sparkSession =>

        val df = getDfCompleteAndInCompleteColumns(sparkSession)

      val checkToSucceed = Check(CheckLevel.Error, "group-1")
        .isComplete("att1")
        .hasCompleteness("att1", _ == 1.0)

      val checkToErrorOut = Check(CheckLevel.Error, "group-2-E")
        .hasCompleteness("att2", _ > 0.8)

      val checkToWarn = Check(CheckLevel.Warning, "group-2-W")
        .hasCompleteness("att2", _ > 0.8)


      assert(VerificationSuite().onData(df).addCheck(checkToSucceed).run().status ==
        CheckStatus.Success)
      assert(VerificationSuite().onData(df).addCheck(checkToErrorOut).run().status ==
        CheckStatus.Error)
      assert(VerificationSuite().onData(df).addCheck(checkToWarn).run.status ==
        CheckStatus.Warning)


      Seq(checkToSucceed, checkToErrorOut).forEachOrder { checks =>
        assert(VerificationSuite().onData(df).addChecks(checks).run.status == CheckStatus.Error)
      }

      Seq(checkToSucceed, checkToWarn).forEachOrder { checks =>
        assert(VerificationSuite().onData(df).addChecks(checks).run.status == CheckStatus.Warning)
      }

      Seq(checkToWarn, checkToErrorOut).forEachOrder { checks =>
        assert(VerificationSuite().onData(df).addChecks(checks).run.status == CheckStatus.Error)
      }

      Seq(checkToSucceed, checkToWarn, checkToErrorOut).forEachOrder { checks =>
        assert(VerificationSuite().onData(df).addChecks(checks).run.status == CheckStatus.Error)
      }
    }

    "accept analysis config for mandatory analysis" in withSparkSession { sparkSession =>

      import sparkSession.implicits._
      val df = getDfFull(sparkSession)

      val checkToSucceed = Check(CheckLevel.Error, "group-1")
        .isComplete("att1") // 1.0
        .hasCompleteness("att1", _ == 1.0) // 1.0

      val analyzers = Size() :: // Analyzer that works on overall document
        Completeness("att2") ::
        Uniqueness("att2") :: // Analyzer that works on single column
        MutualInformation("att1", "att2") :: Nil // Analyzer that works on multi column

      VerificationSuite().onData(df).addCheck(checkToSucceed).addRequiredAnalyzers(analyzers)
        .run match { case result =>
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
    }

    "should run the analysis even there are no constraints" in withSparkSession { sparkSession =>

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

        val analyzers = analyzerToTestReusingResults :: Uniqueness(Seq("item", "att2")) :: Nil

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
        assert(separateResults == runnerResults)
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
          Entity.Dataset, "", "", Try(100.0))))

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

        val verificationResult = VerificationSuite()
          .onData(df)
          .useRepository(repository)
          .addRequiredAnalyzers(analyzers)
          .saveOrAppendResult(saveResultsWithKey)
          .addAnomalyCheck(
            RateOfChangeStrategy(Some(-2.0), Some(2.0)),
            Size(),
            Some(AnomalyCheckConfig(CheckLevel.Warning, "Anomaly check to fail"))
          )
          .addAnomalyCheck(
            RateOfChangeStrategy(Some(-7.0), Some(7.0)),
            Size(),
            Some(AnomalyCheckConfig(CheckLevel.Error, "Anomaly check to succeed",
              Map.empty, Some(0), Some(11)))
          )
          .addAnomalyCheck(
            RateOfChangeStrategy(Some(-7.0), Some(7.0)),
            Size()
          )
          .run()

        val checkResults = verificationResult.checkResults.toSeq

        assert(checkResults(0)._2.status == CheckStatus.Warning)
        assert(checkResults(1)._2.status == CheckStatus.Success)
        assert(checkResults(2)._2.status == CheckStatus.Success)
      }
    }
  }

   /** Run anomaly detection using a repository with some previous analysis results for testing */
  private[this] def evaluateWithRepositoryWithHistory(test: MetricsRepository => Unit): Unit = {

    val repository = new InMemoryMetricsRepository()

    (1 to 2).foreach { timeStamp =>
      val analyzerContext = new AnalyzerContext(Map(
        Size() -> DoubleMetric(Entity.Column, "", "", Success(timeStamp))
      ))
      repository.save(ResultKey(timeStamp, Map("Region" -> "EU")), analyzerContext)
    }

    (3 to 4).foreach { timeStamp =>
      val analyzerContext = new AnalyzerContext(Map(
        Size() -> DoubleMetric(Entity.Column, "", "", Success(timeStamp))
      ))
      repository.save(ResultKey(timeStamp, Map("Region" -> "NA")), analyzerContext)
    }
    test(repository)
  }

  private[this] def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit = {
    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)
  }
}
