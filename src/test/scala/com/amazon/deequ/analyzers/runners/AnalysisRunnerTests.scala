package com.amazon.deequ.analyzers.runners

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers._
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.functions.expr
import org.scalatest.{Matchers, PrivateMethodTester, WordSpec}
import org.apache.spark.sql.functions.udf

import scala.util.Try

class AnalysisRunnerTests extends WordSpec with Matchers with SparkContextSpec with FixtureSupport
  with PrivateMethodTester {

  "AnalysisRunner" should {

    "correctly handle Histograms with binning functions" in withSparkSession { session =>

      val data = getDfWithNumericValues(session)

      val binning = udf { value: Int => value > 2 }

      val analyzer = Histogram("att1", Some(binning))

      val directlyCalculated = analyzer.calculate(data)

      val calculatedViaAnalysis = Analysis(analyzer :: Nil).run(data).metric(analyzer)

      assert(calculatedViaAnalysis.contains(directlyCalculated))
    }

    "join jobs into one for combinable analyzers" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = getDfWithNumericValues(sparkSession)

        val analyzers =
          Completeness("att1") :: Compliance("rule1", "att1 > 3") ::
          Completeness("att2") :: Compliance("rule1", "att1 > 2") ::
          Compliance("rule1", "att2 > 2") ::
          ApproxQuantile("att2", 0.5) :: Nil

        val (separateResults, numSeparateJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = analyzers.map { _.calculate(df) }.toSet
          (results, stat.jobCount)
        }

        val (runnerResults, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = Analysis(analyzers).run(df).allMetrics.toSet
          (results, stat.jobCount)
        }

        assert(numSeparateJobs == analyzers.length)
        assert(numCombinedJobs == 1)
        assert(separateResults == runnerResults)
      }

    "join column grouping analyzers, do grouping once and combine analysis" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

          val df = getDfWithNumericValues(sparkSession)

          val analyzers = Entropy("att1") :: Uniqueness("att1") :: Nil

          val (separateResults, numSeparateJobs) = sparkMonitor.withMonitoringSession { stat =>
            val results = analyzers.map { _.calculate(df) }.toSet
            (results, stat.jobCount)
          }

          val (runnerResults, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
            val results = Analysis(analyzers).run(df).allMetrics.toSet
            (results, stat.jobCount)
          }

          assert(numSeparateJobs == analyzers.length * 2)
          assert(numCombinedJobs == 2)
          assert(separateResults == runnerResults)
       }

    "join column grouping analyzers also for multi column analyzers" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = getDfWithNumericValues(sparkSession)

        val analyzers = Distinctness(Seq("att1", "att2")) :: Uniqueness(Seq("att1", "att2")) :: Nil

        val (separateResults, numSeparateJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = analyzers.map { _.calculate(df) }.toSet
          (results, stat.jobCount)
        }

        val (runnerResults, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = Analysis(analyzers).run(df).allMetrics.toSet
          (results, stat.jobCount)
        }

        assert(numSeparateJobs == analyzers.length * 2)
        assert(numCombinedJobs == 2)
        assert(separateResults == runnerResults)
      }

    "reuse existing results" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = getDfWithNumericValues(sparkSession)

        val analyzerToTestReusingResults = Distinctness(Seq("att1", "att2"))

        val analysisResult = Analysis().addAnalyzer(analyzerToTestReusingResults).run(df)
        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)
        repository.save(resultKey, analysisResult)

        val analyzers = analyzerToTestReusingResults :: Uniqueness(Seq("item", "att2")) :: Nil

        val (separateResults, numSeparateJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = analyzers.map { _.calculate(df) }.toSet
          (results, stat.jobCount)
        }

        val (runnerResults, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = AnalysisRunner.onData(df).useRepository(repository)
            .reuseExistingResultsForKey(resultKey).addAnalyzers(analyzers).run()
            .metricMap.values.toSet

          (results, stat.jobCount)
        }

        assert(numSeparateJobs == analyzers.length * 2)
        assert(numCombinedJobs == 2)
        assert(separateResults == runnerResults)
      }

    "fail if specified when the calculation of new metrics would be needed when " +
      "reusing previous results" in withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = getDfWithNumericValues(sparkSession)

        val analyzerToTestReusingResults = Distinctness(Seq("att1", "att2"))

        val analysisResult = Analysis().addAnalyzer(analyzerToTestReusingResults).run(df)
        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)
        repository.save(resultKey, analysisResult)

        val analyzers = analyzerToTestReusingResults :: Uniqueness(Seq("item", "att2")) ::
          Size() :: Nil

        val (_, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = AnalysisRunner.onData(df).useRepository(repository)
            .reuseExistingResultsForKey(resultKey).addAnalyzers(analyzers).run()
            .metricMap.values.toSet

          (results, stat.jobCount)
        }

        assert(numCombinedJobs == 3)

        val exception = intercept[ReusingNotPossibleResultsMissingException] (
          AnalysisRunner
            .onData(df)
            .useRepository(repository)
            .reuseExistingResultsForKey(resultKey, true)
            .addAnalyzers(analyzers)
            .run()
        )

        assert(exception.getMessage == "Could not find all necessary results in the " +
          "MetricsRepository, the calculation of the metrics for these analyzers " +
          "would be needed: Uniqueness(List(item, att2)), Size(None)")
      }

    "save results if specified" in
      withSparkSession { sparkSession =>

        val df = getDfWithNumericValues(sparkSession)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        val analyzerContext = AnalysisRunner.onData(df).useRepository(repository)
          .addAnalyzers(analyzers).saveOrAppendResult(resultKey).run()

        assert(analyzerContext == repository.loadByKey(resultKey).get)
      }

    "only append results to repository without unnecessarily overwriting existing ones" in
      withSparkSession { sparkSession =>

        val df = getDfWithNumericValues(sparkSession)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        val completeAnalyzerContext = AnalysisRunner.onData(df).useRepository(repository)
          .addAnalyzers(analyzers).saveOrAppendResult(resultKey).run()

        // Calculate and save results for first analyzer
        AnalysisRunner.onData(df).useRepository(repository)
          .addAnalyzer(Size()).saveOrAppendResult(resultKey).run()

        // Calculate and append results for second analyzer
        AnalysisRunner.onData(df).useRepository(repository)
          .addAnalyzer(Completeness("item")).saveOrAppendResult(resultKey).run()

        assert(completeAnalyzerContext == repository.loadByKey(resultKey).get)
      }

    "if there are previous results in the repository new results should pre preferred in case of " +
      "conflicts" in withSparkSession { sparkSession =>

      val df = getDfWithNumericValues(sparkSession)

      val repository = new InMemoryMetricsRepository
      val resultKey = ResultKey(0, Map.empty)

      val analyzers = Size() :: Completeness("item") :: Nil

      val expectedAnalyzerContextOnLoadByKey = AnalysisRunner.onData(df).useRepository(repository)
          .addAnalyzers(analyzers).run()

      val resultWhichShouldBeOverwritten = AnalyzerContext(Map(Size() -> DoubleMetric(
        Entity.Dataset, "", "", Try(100.0))))
      repository.save(resultKey, resultWhichShouldBeOverwritten)

      // This should overwrite the previous Size value
      AnalysisRunner.onData(df).useRepository(repository)
        .addAnalyzers(analyzers).saveOrAppendResult(resultKey).run()

      assert(expectedAnalyzerContextOnLoadByKey == repository.loadByKey(resultKey).get)
    }
  }
}
