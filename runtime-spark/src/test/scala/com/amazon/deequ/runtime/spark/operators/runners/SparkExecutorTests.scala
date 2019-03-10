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

package com.amazon.deequ.runtime.spark.operators.runners

import com.amazon.deequ.{RepositoryOptions, SparkContextSpec}
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.{InMemoryMetricsRepository, ResultKey}
import com.amazon.deequ.runtime.spark.executor._
import com.amazon.deequ.runtime.spark.{OperatorList, SparkEngine}
import com.amazon.deequ.runtime.spark.operators._
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, PrivateMethodTester, WordSpec}
import org.apache.spark.sql.functions.udf

import scala.util.Try

class SparkExecutorTests extends WordSpec with Matchers with SparkContextSpec with FixtureSupport
  with PrivateMethodTester {

  "AnalysisRunner" should {

    "correctly handle Histograms with binning functions" in withSparkSession { session =>

      val data = getDfWithNumericValues(session)

      val binning = udf { value: Int => value > 2 }

      val analyzer = HistogramOp("att1", Some(binning))

      val directlyCalculated = analyzer.calculate(data)

      val calculatedViaAnalysis = OperatorList(analyzer :: Nil).run(data).metric(analyzer)

      assert(calculatedViaAnalysis.contains(directlyCalculated))
    }

    "join jobs into one for combinable analyzers" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = getDfWithNumericValues(sparkSession)

        val analyzers =
          CompletenessOp("att1") :: ComplianceOp("rule1", "att1 > 3") ::
          CompletenessOp("att2") :: ComplianceOp("rule1", "att1 > 2") ::
          ComplianceOp("rule1", "att2 > 2") ::
          ApproxQuantileOp("att2", 0.5) :: Nil

        val (separateResults, numSeparateJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = analyzers.map { _.calculate(df) }.toSet
          (results, stat.jobCount)
        }

        val (runnerResults, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = OperatorList(analyzers).run(df).allMetrics.toSet
          (results, stat.jobCount)
        }

        assert(numSeparateJobs == analyzers.length)
        assert(numCombinedJobs == 1)
        assert(separateResults == runnerResults)
      }

    "join column grouping analyzers, do grouping once and combine analysis" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

          val df = getDfWithNumericValues(sparkSession)

          val analyzers = EntropyOp("att1") :: UniquenessOp("att1") :: Nil

          val (separateResults, numSeparateJobs) = sparkMonitor.withMonitoringSession { stat =>
            val results = analyzers.map { _.calculate(df) }.toSet
            (results, stat.jobCount)
          }

          val (runnerResults, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
            val results = OperatorList(analyzers).run(df).allMetrics.toSet
            (results, stat.jobCount)
          }

          assert(numSeparateJobs == analyzers.length * 2)
          assert(numCombinedJobs == 2)
          assert(separateResults == runnerResults)
       }

    "join column grouping analyzers also for multi column analyzers" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = getDfWithNumericValues(sparkSession)

        val analyzers = DistinctnessOp(Seq("att1", "att2")) :: UniquenessOp(Seq("att1", "att2")) :: Nil

        val (separateResults, numSeparateJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = analyzers.map { _.calculate(df) }.toSet
          (results, stat.jobCount)
        }

        val (runnerResults, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = OperatorList(analyzers).run(df).allMetrics.toSet
          (results, stat.jobCount)
        }

        assert(numSeparateJobs == analyzers.length * 2)
        assert(numCombinedJobs == 2)
        assert(separateResults == runnerResults)
      }

    "reuse existing results" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = getDfWithNumericValues(sparkSession)

        val analyzerToTestReusingResults = DistinctnessOp(Seq("att1", "att2"))

        val analysisResult = OperatorList().addAnalyzer(analyzerToTestReusingResults).run(df)
        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)
        repository.save(resultKey, SparkEngine.analyzerContextToComputedStatistics(analysisResult))

        val analyzers = analyzerToTestReusingResults :: UniquenessOp(Seq("item", "att2")) :: Nil

        val (separateResults, numSeparateJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = analyzers.map { _.calculate(df) }.toSet
          (results, stat.jobCount)
        }

        val (runnerResults, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>

          val repositoryOptions = RepositoryOptions(
            Some(repository),
            reuseExistingResultsForKey = Some(resultKey)
          )

          val results = SparkExecutor.doAnalysisRun(
            df,
            analyzers,
            metricsRepositoryOptions = repositoryOptions
          )
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

        val analyzerToTestReusingResults = DistinctnessOp(Seq("att1", "att2"))

        val analysisResult = OperatorList().addAnalyzer(analyzerToTestReusingResults).run(df)
        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)
        repository.save(resultKey, SparkEngine.analyzerContextToComputedStatistics(analysisResult))

        val analyzers = analyzerToTestReusingResults :: UniquenessOp(Seq("item", "att2")) ::
          SizeOp() :: Nil

      val repositoryOptions = RepositoryOptions(
        Some(repository),
        reuseExistingResultsForKey = Some(resultKey)
      )

        val (_, numCombinedJobs) = sparkMonitor.withMonitoringSession { stat =>
          val results = SparkExecutor.doAnalysisRun(
            df,
            analyzers,
            metricsRepositoryOptions = repositoryOptions
          )
          .metricMap.values.toSet

          (results, stat.jobCount)
        }

        assert(numCombinedJobs == 3)

      val repositoryOptions2 = RepositoryOptions(
        Some(repository),
        reuseExistingResultsForKey = Some(resultKey),
        failIfResultsForReusingMissing = true
      )

        val exception = intercept[ReusingNotPossibleResultsMissingException] (
          SparkExecutor
            .doAnalysisRun(
              df,
              analyzers,
              metricsRepositoryOptions = repositoryOptions2
            )
        )

        assert(exception.getMessage == "Could not find all necessary results in the " +
          "MetricsRepository, the calculation of the metrics for these analyzers " +
          "would be needed: UniquenessOp(List(item, att2)), SizeOp(None)")
      }

//    "save results if specified" in
//      withSparkSession { sparkSession =>
//
//        val df = getDfWithNumericValues(sparkSession)
//
//        val repository = new InMemoryMetricsRepository
//        val resultKey = ResultKey(0, Map.empty)
//
//        val analyzers = SizeOp() :: CompletenessOp("item") :: Nil
//
//        val analyzerContext = AnalysisRunner.onData(df).useRepository(repository)
//          .addAnalyzers(analyzers).saveOrAppendResult(resultKey).run()
//
//        assert(SparkEngine.analyzerContextToComputedStatistics(analyzerContext) == repository.loadByKey(resultKey).get)
//      }

//    "only append results to repository without unnecessarily overwriting existing ones" in
//      withSparkSession { sparkSession =>
//
//        val df = getDfWithNumericValues(sparkSession)
//
//        val repository = new InMemoryMetricsRepository
//        val resultKey = ResultKey(0, Map.empty)
//
//        val analyzers = SizeOp() :: CompletenessOp("item") :: Nil
//
//        val completeAnalyzerContext = AnalysisRunner.onData(df).useRepository(repository)
//          .addAnalyzers(analyzers).saveOrAppendResult(resultKey).run()
//
//        // Calculate and save results for first analyzer
//        AnalysisRunner.onData(df).useRepository(repository)
//          .addAnalyzer(SizeOp()).saveOrAppendResult(resultKey).run()
//
//        // Calculate and append results for second analyzer
//        AnalysisRunner.onData(df).useRepository(repository)
//          .addAnalyzer(CompletenessOp("item")).saveOrAppendResult(resultKey).run()
//
//        val result = SparkEngine.analyzerContextToComputedStatistics(completeAnalyzerContext)
//        assert(result == repository.loadByKey(resultKey).get)
//      }

    "if there are previous results in the repository new results should pre preferred in case of " +
      "conflicts" in withSparkSession { sparkSession =>

      val df = getDfWithNumericValues(sparkSession)

      val repository = new InMemoryMetricsRepository
      val resultKey = ResultKey(0, Map.empty)

      val analyzers = SizeOp() :: CompletenessOp("item") :: Nil

      val expectedAnalyzerContextOnLoadByKey = SparkExecutor.doAnalysisRun(
        df,
        analyzers,
        metricsRepositoryOptions = RepositoryOptions(Some(repository))
      )

      val resultWhichShouldBeOverwritten = OperatorResults(Map(SizeOp() -> DoubleMetric(
        Entity.Dataset, "", "", Try(100.0))))
      repository.save(resultKey, SparkEngine.analyzerContextToComputedStatistics(resultWhichShouldBeOverwritten))

      // This should overwrite the previous Size value
      SparkExecutor.doAnalysisRun(
        df,
        analyzers,
        metricsRepositoryOptions = RepositoryOptions(Some(repository),
          saveOrAppendResultsWithKey = Some(resultKey))
      )


      val result = SparkEngine.analyzerContextToComputedStatistics(expectedAnalyzerContextOnLoadByKey)

      assert(result == repository.loadByKey(resultKey).get)
    }

//    "should write output files to specified locations" in withSparkSession { sparkSession =>
//
//      val df = getDfWithNumericValues(sparkSession)
//
//      val analyzers = SizeOp() :: CompletenessOp("item") :: Nil
//
//      val tempDir = TempFileUtils.tempDir("analysisOuput")
//      val successMetricsPath = tempDir + "/success-metrics.json"
//
//      AnalysisRunner.onData(df)
//        .addAnalyzers(analyzers)
//        .useSparkSession(sparkSession)
//        .saveSuccessMetricsJsonToPath(successMetricsPath)
//        .run()
//
//      DfsUtils.readFromFileOnDfs(sparkSession, successMetricsPath) {
//        inputStream => assert(inputStream.read() > 0)
//      }
//    }
  }
}
