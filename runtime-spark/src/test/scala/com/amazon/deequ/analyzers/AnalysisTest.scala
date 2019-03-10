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

package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.runtime.spark.operators.runners._
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.runtime.spark.OperatorList
import com.amazon.deequ.runtime.spark.executor._
import com.amazon.deequ.runtime.spark.operators._
import com.amazon.deequ.utils.FixtureSupport
import com.amazon.deequ.utils.AssertionUtils.TryUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}

class AnalysisTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Analysis" should {
    "return results for configured analyzers" in withSparkSession { sparkSession =>

      val df = getDfFull(sparkSession)

      val analysisResult = OperatorList()
        .addAnalyzer(SizeOp())
        .addAnalyzer(DistinctnessOp(Seq("item")))
        .addAnalyzer(CompletenessOp("att1"))
        .addAnalyzer(UniquenessOp(Seq("att1", "att2")))
        .run(df)

      val successMetricsAsDataFrame = OperatorResults
        .successMetricsAsDataFrame(sparkSession, analysisResult)

      import sparkSession.implicits._
      val expected = Seq(
        ("Dataset", "*", "Size", 4.0),
        ("Column", "item", "Distinctness", 1.0),
        ("Column", "att1", "Completeness", 1.0),
        ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25))
        .toDF("entity", "instance", "name", "value")

      assertSameRows(successMetricsAsDataFrame, expected)
    }

    "run an individual analyzer only once" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)

      val analysis = OperatorList()
        .addAnalyzer(SizeOp())
        .addAnalyzer(SizeOp())
        .addAnalyzer(SizeOp())

      val analysisResult = analysis.run(df)
      assert(analysisResult.allMetrics.length == 1)
      assert(analysisResult.metric(SizeOp()).get.value.get == 4)
    }

    "return basic statistics" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)

      val analysis = OperatorList()
        .addAnalyzer(MeanOp("att1"))
        .addAnalyzer(StandardDeviationOp("att1"))
        .addAnalyzer(MinimumOp("att1"))
        .addAnalyzer(MaximumOp("att1"))
        .addAnalyzer(ApproxQuantileOp("att1", 0.5))
        .addAnalyzer(ApproxCountDistinctOp("att1"))
        .addAnalyzer(CountDistinctOp("att1"))

      val resultMetrics = analysis.run(df).allMetrics

      assert(resultMetrics.size == analysis.analyzers.size)

      resultMetrics should contain(DoubleMetric(Entity.Column, "Mean", "att1", Success(3.5)))
      resultMetrics should contain(DoubleMetric(Entity.Column, "StandardDeviation", "att1",
        Success(1.707825127659933)))
      resultMetrics should contain(DoubleMetric(Entity.Column, "Minimum", "att1", Success(1.0)))
      resultMetrics should contain(DoubleMetric(Entity.Column, "Maximum", "att1", Success(6.0)))
      resultMetrics should contain(DoubleMetric(Entity.Column, "ApproxCountDistinct", "att1",
        Success(6.0)))
      resultMetrics should contain(DoubleMetric(Entity.Column, "CountDistinct", "att1",
        Success(6.0)))
      resultMetrics should contain(DoubleMetric(Entity.Column, "ApproxQuantile", "att1",
        Success(3.0)))
    }

    "return the proper exception for non existing columns" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)

      val analysis = OperatorList()
        .addAnalyzer(MeanOp("nonExistingColumnName"))

      val analyzerContext = analysis.run(df)

      assert(analyzerContext.metricMap(MeanOp("nonExistingColumnName")).value.compareFailureTypes(
        Failure(new NoSuchColumnException(""))))
    }

    "return the proper exception for columns which need to be numeric but are not" in
      withSparkSession { sparkSession =>
        val df = getDfFull(sparkSession)

        val analysis = OperatorList()
          .addAnalyzer(MeanOp("att2"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(MeanOp("att2")).value.compareFailureTypes(
          Failure(new WrongColumnTypeException(""))))
      }

    "return the proper exception when no columns are specified" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = OperatorList()
          .addAnalyzer(DistinctnessOp(Seq.empty))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(DistinctnessOp(Seq.empty)).value.compareFailureTypes(
          Failure(new NoColumnsSpecifiedException(""))))
      }

    "return the proper exception when the number of specified columns is not correct " +
      "(and should be greater than 1)" in withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = OperatorList()
          .addAnalyzer(MutualInformationOp(Seq("att2")))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(MutualInformationOp(Seq("att2"))).value.compareFailureTypes(
          Failure(new NumberOfSpecifiedColumnsException(""))))
      }

    "return the proper exception when the number of max histogramm bins is too big" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = OperatorList()
          .addAnalyzer(HistogramOp("att2", maxDetailBins = Integer.MAX_VALUE))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(HistogramOp("att2", maxDetailBins = Integer.MAX_VALUE))
          .value.compareFailureTypes(Failure(new IllegalAnalyzerParameterException(""))))
      }

    "return the proper exception when a quantile number is out of range" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = OperatorList()
          .addAnalyzer(ApproxQuantileOp("att2", 1.1))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(ApproxQuantileOp("att2", 1.1))
          .value.compareFailureTypes(Failure(new IllegalAnalyzerParameterException(""))))
      }

    "return the proper exception when a quantile error number is out of range" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = OperatorList()
          .addAnalyzer(ApproxQuantileOp("att2", 0.5, - 0.1))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(ApproxQuantileOp("att2", 0.5, - 0.1))
          .value.compareFailureTypes(Failure(new IllegalAnalyzerParameterException(""))))
      }

    "return a custom generic exception that wraps any other exception that " +
      "occurred while calculating the metric" in withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val meanException = new IllegalArgumentException("-test-mean-failing-")
        val failingMean = new MeanOp("att1") {
          override def fromAggregationResult(result: Row, offset: Int): Option[MeanState] = {
            throw meanException
          }
        }
        val analysis = OperatorList()
            .addAnalyzer(failingMean)

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(failingMean).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(meanException))))
      }
  }

  "Scan-shareable Analysis" should {

    "not fail all the analyzers in case of an exception in producing state from aggregation" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val meanException = new IllegalArgumentException("-test-mean-failing-")
        val failingMean = new MeanOp("att1") {
          override def fromAggregationResult(result: Row, offset: Int): Option[MeanState] = {
            throw meanException
          }
        }

        val analysis = OperatorList()
          .addAnalyzer(failingMean)
          .addAnalyzer(MinimumOp("att1"))
          .addAnalyzer(MaximumOp("att1"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(failingMean).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(meanException))))

        analyzerContext.metricMap(MinimumOp("att1")).value should be
          DoubleMetric(Entity.Column, "Minimum", "att1", Success(1.0))

        analyzerContext.metricMap(MaximumOp("att1")).value should be
          DoubleMetric(Entity.Column, "Maximum", "att1", Success(6.0))
    }

    "fail all analyzers in case of aggregation computation failure" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val aggregationException = new IllegalArgumentException("-test-agg-failing-")
        val aggFailingMean = new MeanOp("att1") {
          override def aggregationFunctions() = throw aggregationException
        }

        val analysis = OperatorList()
          .addAnalyzer(aggFailingMean)
          .addAnalyzer(MinimumOp("att1"))
          .addAnalyzer(MaximumOp("att1"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(aggFailingMean).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))

        assert(analyzerContext.metricMap(MinimumOp("att1")).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))

        assert(analyzerContext.metricMap(MaximumOp("att1")).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))

      }
  }

  "Grouping Analysis" should {

    "not fail all the analyzers in case of an exception in producing state from aggregation" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val distinctnessException = new IllegalArgumentException("-test-distinctness-failing-")
        val failingDistinctness = new DistinctnessOp("att1" :: Nil) {
          override def fromAggregationResult(result: Row, offset: Int): DoubleMetric = {
            throw distinctnessException
          }
        }

        val analysis = OperatorList()
          .addAnalyzer(failingDistinctness)
          .addAnalyzer(EntropyOp("att1"))
          .addAnalyzer(UniquenessOp("att1"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(failingDistinctness).value
          .compareOuterAndInnerFailureTypes(Failure(
            new MetricCalculationRuntimeException(distinctnessException))))

        analyzerContext.metricMap(EntropyOp("att1")).value should be
          DoubleMetric(Entity.Column, "Uniqueness", "att1", Success(1.0))

        analyzerContext.metricMap(UniquenessOp("att1")).value should be
          DoubleMetric(Entity.Column, "Maximum", "att1", Success(6.0))
      }

    "fail all analyzers in case of aggregation computation failure" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val aggregationException = new IllegalArgumentException("-test-agg-failing-")
        val failingDistinctness = new DistinctnessOp("att1" :: Nil) {
          override def aggregationFunctions(numRows: Long) = throw aggregationException
        }

        val analysis = OperatorList()
          .addAnalyzer(failingDistinctness)
          .addAnalyzer(EntropyOp("att1"))
          .addAnalyzer(UniquenessOp("att1"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(failingDistinctness).value
          .compareOuterAndInnerFailureTypes(Failure(
            new MetricCalculationRuntimeException(aggregationException))))

        assert(analyzerContext.metricMap(EntropyOp("att1")).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))

        assert(analyzerContext.metricMap(UniquenessOp("att1")).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))
      }
  }

  private[this] def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit = {
    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)
  }
}
