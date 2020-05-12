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
import com.amazon.deequ.analyzers.runners._
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.utils.FixtureSupport
import com.amazon.deequ.utils.AssertionUtils.TryUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}

class AnalysisTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Analysis" should {
    "return results for configured analyzers" in withSparkSession { sparkSession =>

      val df = getDfFull(sparkSession)

      val analysisResult = Analysis()
        .addAnalyzer(Size())
        .addAnalyzer(Distinctness("item"))
        .addAnalyzer(Completeness("att1"))
        .addAnalyzer(Uniqueness(Seq("att1", "att2")))
        .run(df)

      val successMetricsAsDataFrame = AnalyzerContext
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

    "return results for configured analyzers in case insensitive manner" in
      withSparkSession { sparkSession =>

        sparkSession.sqlContext.setConf("spark.sql.caseSensitive", "false")

        val df = getDfFull(sparkSession)

        val analysisResult = Analysis()
          .addAnalyzer(Size())
          .addAnalyzer(Distinctness("ITEM"))
          .addAnalyzer(Completeness("ATT1"))
          .addAnalyzer(Uniqueness(Seq("ATT1", "ATT2")))
          .run(df)

        val successMetricsAsDataFrame = AnalyzerContext
          .successMetricsAsDataFrame(sparkSession, analysisResult)

        import sparkSession.implicits._
        val expected = Seq(
          ("Dataset", "*", "Size", 4.0),
          ("Column", "ITEM", "Distinctness", 1.0),
          ("Column", "ATT1", "Completeness", 1.0),
          ("Mutlicolumn", "ATT1,ATT2", "Uniqueness", 0.25))
          .toDF("entity", "instance", "name", "value")
        assertSameRows(successMetricsAsDataFrame, expected)
      }

    "return basic statistics" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)

      val analysis = Analysis()
        .addAnalyzer(Mean("att1"))
        .addAnalyzer(StandardDeviation("att1"))
        .addAnalyzer(Minimum("att1"))
        .addAnalyzer(Maximum("att1"))
        .addAnalyzer(ApproxQuantile("att1", 0.5))
        .addAnalyzer(ApproxCountDistinct("att1"))
        .addAnalyzer(CountDistinct("att1"))

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
      resultMetrics should contain(DoubleMetric(Entity.Column, "ApproxQuantile-0.5", "att1",
        Success(3.0)))
    }

    "return string length statistics" in withSparkSession { sparkSession =>
      val df = getDfWithVariableStringLengthValues(sparkSession)

      val analysis = Analysis()
        .addAnalyzer(MaxLength("att1"))
        .addAnalyzer(MinLength("att1"))

      val resultMetrics = analysis.run(df).allMetrics

      assert(resultMetrics.size == analysis.analyzers.size)

      resultMetrics should contain(DoubleMetric(Entity.Column, "MaxLength", "att1",
        Success(4.0)))
      resultMetrics should contain(DoubleMetric(Entity.Column, "MinLength", "att1",
        Success(0.0)))
    }

    "return the proper exception for non existing columns" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)

      val analysis = Analysis()
        .addAnalyzer(Mean("nonExistingColumnName"))

      val analyzerContext = analysis.run(df)

      assert(analyzerContext.metricMap(Mean("nonExistingColumnName")).value.compareFailureTypes(
        Failure(new NoSuchColumnException(""))))
    }

    "return the proper exception for columns which need to be numeric but are not" in
      withSparkSession { sparkSession =>
        val df = getDfFull(sparkSession)

        val analysis = Analysis()
          .addAnalyzer(Mean("att2"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(Mean("att2")).value.compareFailureTypes(
          Failure(new WrongColumnTypeException(""))))
      }

    "return the proper exception when no columns are specified" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = Analysis()
          .addAnalyzer(Distinctness(Seq.empty))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(Distinctness(Seq.empty)).value.compareFailureTypes(
          Failure(new NoColumnsSpecifiedException(""))))
      }

    "return the proper exception when the number of specified columns is not correct " +
      "(and should be greater than 1)" in withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = Analysis()
          .addAnalyzer(MutualInformation(Seq("att2")))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(MutualInformation(Seq("att2"))).value.compareFailureTypes(
          Failure(new NumberOfSpecifiedColumnsException(""))))
      }

    "return the proper exception when the number of max histogram bins is too big" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = Analysis()
          .addAnalyzer(Histogram("att2", maxDetailBins = Integer.MAX_VALUE))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(Histogram("att2", maxDetailBins = Integer.MAX_VALUE))
          .value.compareFailureTypes(Failure(new IllegalAnalyzerParameterException(""))))
      }

    "return the proper exception when a quantile number is out of range" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = Analysis()
          .addAnalyzer(ApproxQuantile("att2", 1.1))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(ApproxQuantile("att2", 1.1))
          .value.compareFailureTypes(Failure(new IllegalAnalyzerParameterException(""))))
      }

    "return the proper exception when a quantile error number is out of range" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val analysis = Analysis()
          .addAnalyzer(ApproxQuantile("att2", 0.5, - 0.1))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(ApproxQuantile("att2", 0.5, - 0.1))
          .value.compareFailureTypes(Failure(new IllegalAnalyzerParameterException(""))))
      }

    "return a custom generic exception that wraps any other exception that " +
      "occurred while calculating the metric" in withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val meanException = new IllegalArgumentException("-test-mean-failing-")
        val failingMean = new Mean("att1") {
          override def fromAggregationResult(result: Row, offset: Int): Option[MeanState] = {
            throw meanException
          }
        }
        val analysis = Analysis()
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
        val failingMean = new Mean("att1") {
          override def fromAggregationResult(result: Row, offset: Int): Option[MeanState] = {
            throw meanException
          }
        }

        val analysis = Analysis()
          .addAnalyzer(failingMean)
          .addAnalyzer(Minimum("att1"))
          .addAnalyzer(Maximum("att1"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(failingMean).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(meanException))))

        analyzerContext.metricMap(Minimum("att1")).value should be
          DoubleMetric(Entity.Column, "Minimum", "att1", Success(1.0))

        analyzerContext.metricMap(Maximum("att1")).value should be
          DoubleMetric(Entity.Column, "Maximum", "att1", Success(6.0))
    }

    "fail all analyzers in case of aggregation computation failure" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val aggregationException = new IllegalArgumentException("-test-agg-failing-")
        val aggFailingMean = new Mean("att1") {
          override def aggregationFunctions() = throw aggregationException
        }

        val analysis = Analysis()
          .addAnalyzer(aggFailingMean)
          .addAnalyzer(Minimum("att1"))
          .addAnalyzer(Maximum("att1"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(aggFailingMean).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))

        assert(analyzerContext.metricMap(Minimum("att1")).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))

        assert(analyzerContext.metricMap(Maximum("att1")).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))

      }
  }

  "Grouping Analysis" should {

    "not fail all the analyzers in case of an exception in producing state from aggregation" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val distinctnessException = new IllegalArgumentException("-test-distinctness-failing-")
        val failingDistinctness = new Distinctness("att1" :: Nil) {
          override def fromAggregationResult(result: Row, offset: Int): DoubleMetric = {
            throw distinctnessException
          }
        }

        val analysis = Analysis()
          .addAnalyzer(failingDistinctness)
          .addAnalyzer(Entropy("att1"))
          .addAnalyzer(Uniqueness("att1"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(failingDistinctness).value
          .compareOuterAndInnerFailureTypes(Failure(
            new MetricCalculationRuntimeException(distinctnessException))))

        analyzerContext.metricMap(Entropy("att1")).value should be
          DoubleMetric(Entity.Column, "Uniqueness", "att1", Success(1.0))

        analyzerContext.metricMap(Uniqueness("att1")).value should be
          DoubleMetric(Entity.Column, "Maximum", "att1", Success(6.0))
      }

    "fail all analyzers in case of aggregation computation failure" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)

        val aggregationException = new IllegalArgumentException("-test-agg-failing-")
        val failingDistinctness = new Distinctness("att1" :: Nil) {
          override def aggregationFunctions(numRows: Long) = throw aggregationException
        }

        val analysis = Analysis()
          .addAnalyzer(failingDistinctness)
          .addAnalyzer(Entropy("att1"))
          .addAnalyzer(Uniqueness("att1"))

        val analyzerContext = analysis.run(df)

        assert(analyzerContext.metricMap(failingDistinctness).value
          .compareOuterAndInnerFailureTypes(Failure(
            new MetricCalculationRuntimeException(aggregationException))))

        assert(analyzerContext.metricMap(Entropy("att1")).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))

        assert(analyzerContext.metricMap(Uniqueness("att1")).value.compareOuterAndInnerFailureTypes(
          Failure(new MetricCalculationRuntimeException(aggregationException))))
      }
  }

  private[this] def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit = {
    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)
  }
}
