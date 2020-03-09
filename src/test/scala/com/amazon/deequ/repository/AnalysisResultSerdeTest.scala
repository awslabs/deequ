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

package com.amazon.deequ.repository

import java.time.{LocalDate, ZoneOffset}

import com.amazon.deequ.analyzers.{Compliance, DataType, Entropy, Histogram, Maximum, Mean, Minimum, MutualInformation, StandardDeviation, Uniqueness, _}
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics._
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest._
import AnalysisResultSerde._
import com.amazon.deequ.SparkContextSpec

import scala.util.{Failure, Success}

class AnalysisResultSerdeTest extends FlatSpec with Matchers {

  "analysis results serialization with successful Values" should "work" in {

    val analyzerContextWithAllSuccValues = new AnalyzerContext(Map(
      Size() -> DoubleMetric(Entity.Column, "Size", "*", Success(5.0)),
      Completeness("ColumnA") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Compliance("rule1", "att1 > 3") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      ApproxCountDistinct("columnA", Some("test")) ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      CountDistinct(Seq("columnA", "columnB")) ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Distinctness(Seq("columnA", "columnB")) ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Correlation("firstColumn", "secondColumn", Some("test")) ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      UniqueValueRatio(Seq("columnA", "columnB")) ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Correlation("firstColumn", "secondColumn", Some("test")) ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Uniqueness("ColumnA") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Uniqueness(Seq("ColumnA", "ColumnB")) ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Histogram("ColumnA") ->
        HistogramMetric("ColumnA", Success(Distribution(
          Map("some" -> DistributionValue(10, 0.5)), 10))),
      Histogram ("ColumnA", None) ->
        HistogramMetric("ColumnA", Success(Distribution(
          Map("some" -> DistributionValue(10, 0.5), "other" -> DistributionValue(0, 0)), 10))),
      Histogram("ColumnA", None, 5) ->
        HistogramMetric("ColumnA", Success(Distribution(
          Map("some" -> DistributionValue(10, 0.5)), 10))),
      Entropy("ColumnA") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      MutualInformation(Seq("ColumnA", "ColumnB")) ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Minimum("ColumnA") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Maximum("ColumnA") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Mean("ColumnA") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      Sum("ColumnA") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      StandardDeviation("ColumnA") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      DataType("ColumnA") ->
        DoubleMetric(Entity.Column, "Completeness", "ColumnA", Success(5.0)),
      MinLength("ColumnA") ->
        DoubleMetric(Entity.Column, "MinLength", "ColumnA", Success(5.0)),
      MaxLength("ColumnA") ->
        DoubleMetric(Entity.Column, "MaxLength", "ColumnA", Success(5.0))
    ))

    val dateTime = LocalDate.of(2017, 10, 14).atTime(10, 10, 10)
        .toEpochSecond(ZoneOffset.UTC)
    val resultKeyOne = ResultKey(dateTime, Map("Region" -> "EU"))
    val resultKeyTwo = ResultKey(dateTime, Map("Region" -> "NA"))

    val analysisResultOne = AnalysisResult(resultKeyOne, analyzerContextWithAllSuccValues)

    val analysisResultTwo = AnalysisResult(resultKeyTwo, analyzerContextWithAllSuccValues)

    assertCorrectlyConvertsAnalysisResults(Seq(analysisResultOne, analysisResultTwo))
  }

  "analysis results serialization " should "also work for regex with broken ==" in {

    val dateTime = LocalDate.of(2017, 10, 14).atTime(10, 10, 10)
      .toEpochSecond(ZoneOffset.UTC)
    val resultKeyOne = ResultKey(dateTime, Map("Region" -> "EU"))

    val analyzer = PatternMatch("patternRule1", Patterns.EMAIL)
    val metric = DoubleMetric(Entity.Column, "PatternMatch", "ColumnA", Success(5.0))

    val result = AnalysisResult(resultKeyOne, AnalyzerContext(Map(analyzer -> metric)))

    val clonedResult = deserialize(serialize(Seq(result))).head

    val (clonedAnalyzer, clonedMetric) = clonedResult.analyzerContext.metricMap
      .collect { case (analyzer: PatternMatch, metric: DoubleMetric) =>
        analyzer -> metric
      }
      .head

    assert(analyzer.column == clonedAnalyzer.column)
    assert(analyzer.pattern.toString() == clonedAnalyzer.pattern.toString())
    assert(analyzer.where == clonedAnalyzer.where)

    assert(metric == clonedMetric)
  }

  "analysis results serialization with mixed Values" should "fail" in {
    val sampleException = new IllegalArgumentException(s"Some")

    val analyzerContextWithMixedValues = new AnalyzerContext(
      Map(
        Size() -> DoubleMetric(Entity.Column, "Size", "*", Success(5.0)),
        Completeness("ColumnA") ->
            DoubleMetric(Entity.Column, "Completeness", "ColumnA", Failure(sampleException))
      )
    )

    val dateTime = LocalDate.of(2017, 10, 14).atTime(10, 10, 10)
        .toEpochSecond(ZoneOffset.UTC)
    val resultKeyOne = ResultKey(dateTime, Map("Region" -> "EU"))
    val resultKeyTwo = ResultKey(dateTime, Map("Region" -> "NA"))

    val analysisResultOne = AnalysisResult(resultKeyOne, analyzerContextWithMixedValues)

    val analysisResultTwo = AnalysisResult(resultKeyTwo, analyzerContextWithMixedValues)

    assertCorrectlyConvertsAnalysisResults(Seq(analysisResultOne, analysisResultTwo),
        shouldFail = true)
  }

  "serialization of ApproxQuantile" should "correctly restore it" in {

    val analyzer = ApproxQuantile("col", 0.5, relativeError = 0.2)
    val metric = DoubleMetric(Entity.Column, "ApproxQuantile", "col", Success(0.5))
    val context = AnalyzerContext(Map(analyzer -> metric))
    val result = new AnalysisResult(ResultKey(0), context)

    assertCorrectlyConvertsAnalysisResults(Seq(result))
  }

  "serialization of ApproxQuantiles" should "correctly restore it" in {

    val quartiles = Map(
      "0.25" -> 10.0,
      "0.5" -> 20.0,
      "0.75" -> 30.0)

    val analyzer = ApproxQuantiles("col", Seq(0.25, 0.5, 0.75), relativeError = 0.2)
    val metric = KeyedDoubleMetric(Entity.Column, "ApproxQuantiles", "col", Success(quartiles))
    val context = AnalyzerContext(Map(analyzer -> metric))
    val result = new AnalysisResult(ResultKey(0), context)

    assertCorrectlyConvertsAnalysisResults(Seq(result))
  }

  def assertCorrectlyConvertsAnalysisResults(
      analysisResults: Seq[AnalysisResult],
      shouldFail: Boolean = false)
    : Unit = {

    if (shouldFail) {
      intercept[IllegalArgumentException](
        AnalysisResultSerde.serialize(analysisResults))
    } else {
      val serialized = serialize(analysisResults)

      val deserialized = deserialize(serialized)

      assert(analysisResults == deserialized)
    }
  }
}

class SimpleResultSerdeTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport{

  "serialize and deserialize success metric results with tags" in
    withSparkSession { sparkSession =>

      val df = getDfFull(sparkSession)

      val analysis = Analysis()
        .addAnalyzer(Size())
        .addAnalyzer(Distinctness("item"))
        .addAnalyzer(Completeness("att1"))
        .addAnalyzer(Uniqueness("att1"))
        .addAnalyzer(Distinctness("att1"))
        .addAnalyzer(Completeness("att2"))
        .addAnalyzer(Uniqueness("att2"))
        .addAnalyzer(MutualInformation("att1", "att2"))
        .addAnalyzer(MinLength("att1"))
        .addAnalyzer(MaxLength("att1"))

      val analysisContext = analysis.run(df)

      val date = LocalDate.of(2017, 10, 14).atTime(10, 10, 10)
        .toEpochSecond(ZoneOffset.UTC)

      val sucessMetricsResultJson = AnalysisResult
        .getSuccessMetricsAsJson(
          AnalysisResult(ResultKey(date, Map("Region" -> "EU")), analysisContext)
        )

      val expected =
        """[{"dataset_date":1507975810,"entity":"Column","region":"EU",
          |"instance":"item","name":"Distinctness","value":1.0},
          |{"dataset_date":1507975810,"entity":"Column","region":"EU",
          |"instance":"att2","name":"Completeness","value":1.0},
          |{"dataset_date":1507975810,"entity":"Column","region":"EU",
          |"instance":"att1","name":"Completeness","value":1.0},
          |{"dataset_date":1507975810,"entity":"Column","region":"EU",
          |"instance":"att1","name":"MinLength","value":1.0},
          |{"dataset_date":1507975810,"entity":"Column","region":"EU",
          |"instance":"att1","name":"MaxLength","value":1.0},
          |{"dataset_date":1507975810,"entity":"Mutlicolumn","region":"EU",
          |"instance":"att1,att2","name":"MutualInformation","value":0.5623351446188083},
          |{"dataset_date":1507975810,"entity":"Dataset","region":"EU",
          |"instance":"*","name":"Size","value":4.0},
          |{"dataset_date":1507975810,"entity":"Column","region":"EU",
          |"instance":"att1","name":"Uniqueness","value":0.25},
          |{"dataset_date":1507975810,"entity":"Column","region":"EU",
          |"instance":"att1","name":"Distinctness","value":0.5},
          |{"dataset_date":1507975810,"entity":"Column","region":"EU",
          |"instance":"att2","name":"Uniqueness","value":0.25}]"""
            .stripMargin.replaceAll("\n", "")

      // ordering of map entries is not guaranteed, so comparing strings is not an option
      assert(SimpleResultSerde.deserialize(sucessMetricsResultJson) ==
        SimpleResultSerde.deserialize(expected))
    }
}
