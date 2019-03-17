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

package com.amazon.deequ.profiles

import com.amazon.deequ.{ComputedStatistics, SparkContextSpec, VerificationSuite}
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.InMemoryMetricsRepository
import com.amazon.deequ.runtime.spark.executor.ReusingNotPossibleResultsMissingException
import com.amazon.deequ.runtime.spark.{SparkDataset, SparkEngine}
import com.amazon.deequ.serialization.json.JsonSerializer
import com.amazon.deequ.statistics.{Completeness, Size}
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

class RDDColumnProfilerRunnerTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "Column Profiler runner" should {

    "save and reuse existing results for column profile runs" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = SparkDataset(getDfWithNumericValues(sparkSession))

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        ColumnProfilerRunner().onData(df)
          .useRepository(repository)
          .saveOrAppendResult(resultKey)
          .run()

        val (separateResults: ColumnProfiles, jobNumberAllCalculations) = sparkMonitor
          .withMonitoringSession { stat =>
            val results = ColumnProfilerRunner().onData(df).run()

            (results, stat.jobCount)
          }

        val (resultsReusingMetrics: ColumnProfiles, jobNumberReusing) = sparkMonitor
          .withMonitoringSession { stat =>
            val results = ColumnProfilerRunner().onData(df).useRepository(repository)
              .reuseExistingResultsForKey(resultKey).run()

            (results, stat.jobCount)
          }

        assert(jobNumberAllCalculations == 2)
        assert(jobNumberReusing == 0)
        assertConstraintSuggestionResultsEquals(separateResults, resultsReusingMetrics)
      }

    "save results if specified so they can be reused by other runners" in
      withSparkSession { sparkSession =>

        val engine = SparkEngine(sparkSession)
        val df = SparkDataset(getDfWithNumericValues(sparkSession))

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        ColumnProfilerRunner().onData(df).useRepository(repository)
          .saveOrAppendResult(resultKey).run()

        val analyzerContext = engine.compute(df, analyzers)

        assert(analyzerContext.metricMap.size == 2)
        assert(analyzerContext.metricMap.toSet
          .subsetOf(repository.loadByKey(resultKey).get.metricMap.toSet))
      }

    "only append results to repository without unnecessarily overwriting existing ones" in
      withSparkSession { sparkSession =>

        val df = SparkDataset(getDfWithNumericValues(sparkSession))

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        val completeMetricResults = VerificationSuite().onData(df).useRepository(repository)
          .addRequiredAnalyzers(analyzers).saveOrAppendResult(resultKey).run().metrics

        val completeAnalyzerContext = ComputedStatistics(completeMetricResults)

        // Calculate and save results for first analyzer
        ColumnProfilerRunner().onData(df).useRepository(repository)
          .saveOrAppendResult(resultKey).run()

        // Calculate and append results for second analyzer
        ColumnProfilerRunner().onData(df).useRepository(repository)
          .saveOrAppendResult(resultKey).run()

        assert(completeAnalyzerContext.metricMap.size == 2)
        assert(completeAnalyzerContext.metricMap.toSet
          .subsetOf(repository.loadByKey(resultKey).get.metricMap.toSet))
      }

    "if there are previous results in the repository new results should pre preferred in case of " +
      "conflicts" in withSparkSession { sparkSession =>

      val engine = SparkEngine(sparkSession)
      val df = SparkDataset(getDfWithNumericValues(sparkSession))

      val repository = new InMemoryMetricsRepository
      val resultKey = ResultKey(0, Map.empty)

      val analyzers = Size() :: Completeness("item") :: Nil

      val expectedAnalyzerContextOnLoadByKey = engine.compute(df, analyzers)

      val resultWhichShouldBeOverwritten = ComputedStatistics(Map(Size() -> DoubleMetric(
        Entity.Dataset, "", "", Try(100.0))))

      repository.save(resultKey, resultWhichShouldBeOverwritten)

      // This should overwrite the previous Size value
      ColumnProfilerRunner().onData(df)
        .useRepository(repository)
        .saveOrAppendResult(resultKey).run()

      assert(expectedAnalyzerContextOnLoadByKey.metricMap.size == 2)
      assert(expectedAnalyzerContextOnLoadByKey.metricMap.toSet
          .subsetOf(repository.loadByKey(resultKey).get.metricMap.toSet))
    }

//    "should write output files to specified locations" in withSparkSession { sparkSession =>
//
//      val engine = SparkEngine(sparkSession)
//      val df = SparkDataset(getDfWithNumericValues(sparkSession))
//
//      val tempDir = TempFileUtils.tempDir("constraintSuggestionOuput")
//      val columnProfilesPath = tempDir + "/column-profiles.json"
//
//      ColumnProfilerRunner().onData(df, engine)
//        .saveColumnProfilesJsonToPath(columnProfilesPath)
//        .run()
//
//      DfsUtils.readFromFileOnDfs(sparkSession, columnProfilesPath) {
//        inputStream => assert(inputStream.read() > 0)
//      }
//    }

    "fail if specified when the calculation of new metrics would be needed when " +
      "reusing previous results" in withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

      val df = SparkDataset(getDfWithNumericValues(sparkSession))

      intercept[ReusingNotPossibleResultsMissingException](
        ColumnProfilerRunner()
          .onData(df)
          .useRepository(new InMemoryMetricsRepository())
          .reuseExistingResultsForKey(ResultKey(0), true)
          .run()
      )
    }
  }

  private[this] def assertConstraintSuggestionResultsEquals(
    expectedResult: ColumnProfiles,
    actualResult: ColumnProfiles): Unit = {

    assert(expectedResult == actualResult)

    val expectedConstraintSuggestionJson = JsonSerializer.columnsProfiles(
      expectedResult.profiles.values.toSeq)

    val actualConstraintSuggestionJson = JsonSerializer.columnsProfiles(
      actualResult.profiles.values.toSeq)

    assert(expectedConstraintSuggestionJson == actualConstraintSuggestionJson)
  }
}
