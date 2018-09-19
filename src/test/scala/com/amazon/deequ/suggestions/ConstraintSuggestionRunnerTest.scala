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

package com.amazon.deequ.suggestions

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext, ReusingNotPossibleResultsMissingException}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.io.DfsUtils
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.utils.{FixtureSupport, TempFileUtils}
import org.scalatest.{Matchers, WordSpec}
import scala.util.Try

class ConstraintSuggestionRunnerTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "Constraint Suggestion Suite" should {

    "save and reuse existing results for constraint suggestion runs" in
      withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

        val df = getDfWithNumericValues(sparkSession)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        ConstraintSuggestionRunner().onData(df).addConstraintRules(Rules.ALL)
          .useRepository(repository)
          .saveOrAppendResult(resultKey)
          .run()

        val (separateResults: ConstraintSuggestionResult, jobNumberAllCalculations) = sparkMonitor
          .withMonitoringSession { stat =>
            val results = ConstraintSuggestionRunner().onData(df)
              .addConstraintRules(Rules.ALL).run()

            (results, stat.jobCount)
          }

        val (resultsReusingMetrics: ConstraintSuggestionResult, jobNumberReusing) = sparkMonitor
          .withMonitoringSession { stat =>
            val results = ConstraintSuggestionRunner().onData(df).useRepository(repository)
              .reuseExistingResultsForKey(resultKey).addConstraintRules(Rules.ALL).run()

            (results, stat.jobCount)
          }

        assert(jobNumberAllCalculations == 2)
        assert(jobNumberReusing == 0)
        assertConstraintSuggestionResultsEquals(separateResults, resultsReusingMetrics)
      }

    "save results if specified so they can be reused by other runners" in
      withSparkSession { sparkSession =>

        val df = getDfWithNumericValues(sparkSession)

        val repository = new InMemoryMetricsRepository
        val resultKey = ResultKey(0, Map.empty)

        val analyzers = Size() :: Completeness("item") :: Nil

        ConstraintSuggestionRunner().onData(df).useRepository(repository)
          .addConstraintRules(Rules.ALL).saveOrAppendResult(resultKey).run()

        val analyzerContext = AnalysisRunner.onData(df).addAnalyzers(analyzers).run()

        assert(analyzerContext.metricMap.size == 2)
        assert(analyzerContext.metricMap.toSet
          .subsetOf(repository.loadByKey(resultKey).get.metricMap.toSet))
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
        ConstraintSuggestionRunner().onData(df).useRepository(repository)
          .saveOrAppendResult(resultKey).run()

        // Calculate and append results for second analyzer
        ConstraintSuggestionRunner().onData(df).useRepository(repository)
          .saveOrAppendResult(resultKey).run()

        assert(completeAnalyzerContext.metricMap.size == 2)
        assert(completeAnalyzerContext.metricMap.toSet
          .subsetOf(repository.loadByKey(resultKey).get.metricMap.toSet))
      }

    "if there are previous results in the repository new results should pre preferred in case of " +
      "conflicts" in withSparkSession { sparkSession =>

      val df = getDfWithNumericValues(sparkSession)

      val repository = new InMemoryMetricsRepository
      val resultKey = ResultKey(0, Map.empty)

      val analyzers = Size() :: Completeness("item") :: Nil

      val expectedAnalyzerContextOnLoadByKey = AnalysisRunner
        .onData(df)
        .addAnalyzers(analyzers)
        .run()

      val resultWhichShouldBeOverwritten = AnalyzerContext(Map(Size() -> DoubleMetric(
        Entity.Dataset, "", "", Try(100.0))))

      repository.save(resultKey, resultWhichShouldBeOverwritten)

      // This should overwrite the previous Size value
      ConstraintSuggestionRunner().onData(df).useRepository(repository)
        .saveOrAppendResult(resultKey).run()

      assert(expectedAnalyzerContextOnLoadByKey.metricMap.size == 2)
      assert(expectedAnalyzerContextOnLoadByKey.metricMap.toSet
          .subsetOf(repository.loadByKey(resultKey).get.metricMap.toSet))
    }

    "should write output files to specified locations" in withSparkSession { sparkSession =>

      val df = getDfWithNumericValues(sparkSession)

      val tempDir = TempFileUtils.tempDir("constraintSuggestionOuput")
      val columnProfilesPath = tempDir + "/column-profiles.json"
      val constraintSuggestionsPath = tempDir + "/constraint-suggestions.json"
      val evaluationResultsPath = tempDir + "/evaluation-results.json"

      ConstraintSuggestionRunner().onData(df)
        .addConstraintRules(Rules.ALL)
        .useSparkSession(sparkSession)
        .saveColumnProfilesJsonToPath(columnProfilesPath)
        .saveConstraintSuggestionsJsonToPath(constraintSuggestionsPath)
        .useTrainTestSplitWithTestsetRatio(0.1, Some(0))
        .saveEvaluationResultsJsonToPath(evaluationResultsPath)
        .run()

      DfsUtils.readFromFileOnDfs(sparkSession, columnProfilesPath) {
        inputStream => assert(inputStream.read() > 0)
      }
      DfsUtils.readFromFileOnDfs(sparkSession, constraintSuggestionsPath) {
        inputStream => assert(inputStream.read() > 0)
      }
      DfsUtils.readFromFileOnDfs(sparkSession, evaluationResultsPath) {
        inputStream => assert(inputStream.read() > 0)
      }
    }

    "fail if specified when the calculation of new metrics would be needed when " +
      "reusing previous results" in withMonitorableSparkSession { (sparkSession, sparkMonitor) =>

      val df = getDfWithNumericValues(sparkSession)

      intercept[ReusingNotPossibleResultsMissingException](
        ConstraintSuggestionRunner()
          .onData(df)
          .addConstraintRules(Rules.ALL)
          .useRepository(new InMemoryMetricsRepository())
          .reuseExistingResultsForKey(ResultKey(0), true)
          .run()
      )
    }
  }

  private[this] def assertConstraintSuggestionResultsEquals(
    expectedResult: ConstraintSuggestionResult,
    actualResult: ConstraintSuggestionResult): Unit = {

    assert(expectedResult.columnProfiles == actualResult.columnProfiles)

    val expectedConstraintSuggestionJson = ConstraintSuggestionResult
      .getConstraintSuggestionsAsJson(expectedResult)


    val actualConstraintSuggestionJson = ConstraintSuggestionResult
      .getConstraintSuggestionsAsJson(actualResult)

    assert(expectedConstraintSuggestionJson == actualConstraintSuggestionJson)
  }
}
