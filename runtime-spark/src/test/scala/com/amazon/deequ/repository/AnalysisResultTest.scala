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

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import org.apache.spark.sql.{DataFrame, SparkSession}

class AnalysisResultTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  private[this] val DATE_ONE = createDate(2017, 10, 14)

  private[this] val REGION_EU = Map("Region" -> "EU")
  private[this] val REGION_EU_INVALID = Map("Re%%^gion!/" -> "EU")
  private[this] val DUPLICATE_COLUMN_NAME = Map("name" -> "EU")
  private[this] val MULTIPLE_TAGS = Map("Region" -> "EU", "Data Set Name" -> "Some")

  "AnalysisResult" should {

    "correctly return a DataFrame that is formatted as expected" in withSparkSession { session =>

      evaluate(session) { results =>

        val resultKey = ResultKey(DATE_ONE, REGION_EU)

        val analysisResultsAsDataFrame = AnalysisResult
            .getSuccessMetricsAsDataFrame(session, AnalysisResult(resultKey, results))

        import session.implicits._

        val expected = Seq(
          ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU"),
          ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"),
          ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"))
          .toDF("entity", "instance", "name", "value", "dataset_date", "region")

        assertSameRows(analysisResultsAsDataFrame, expected)
      }
    }

    "correctly return Json that is formatted as expected" in withSparkSession { session =>

        evaluate(session) { results =>

          val resultKey = ResultKey(DATE_ONE, REGION_EU)

          val analysisResultsAsJson = AnalysisResult
            .getSuccessMetricsAsJson(AnalysisResult(resultKey, results))

          val expected =
            s"""[{"entity":"Dataset","instance":"*","name":"Size","value":4.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25,
              |"region":"EU", "dataset_date":$DATE_ONE}]"""
              .stripMargin.replaceAll("\n", "")

          assertSameJson(analysisResultsAsJson, expected)
        }
    }

    "only include requested metrics in returned DataFrame" in withSparkSession { session =>

      evaluate(session) { results =>

        val resultKey = ResultKey(DATE_ONE, REGION_EU)

        val metricsForAnalyzers = Seq(Completeness("att1"), Uniqueness(Seq("att1", "att2")))

        val analysisResultsAsDataFrame = AnalysisResult
          .getSuccessMetricsAsDataFrame(session, AnalysisResult(resultKey, results),
            metricsForAnalyzers)

        import session.implicits._

        val expected = Seq(
          ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"))
          .toDF("entity", "instance", "name", "value", "dataset_date", "region")

        assertSameRows(analysisResultsAsDataFrame, expected)
      }
    }

    "only include requested metrics in returned Json" in withSparkSession { session =>

        evaluate(session) { results =>

          val resultKey = ResultKey(DATE_ONE, REGION_EU)

          val metricsForAnalyzers = Seq(Completeness("att1"), Uniqueness(Seq("att1", "att2")))

          val analysisResultsAsJson = AnalysisResult
            .getSuccessMetricsAsJson(AnalysisResult(resultKey, results),
              metricsForAnalyzers)


          val expected =
            s"""[{"entity":"Column","instance":"att1","name":"Completeness","value":1.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25,
              |"region":"EU", "dataset_date":$DATE_ONE}]"""
              .stripMargin.replaceAll("\n", "")

          assertSameJson(analysisResultsAsJson, expected)
        }
      }

    "turn tagNames into valid columnNames in returned DataFrame" in withSparkSession { session =>

      evaluate(session) { results =>

        val resultKey = ResultKey(DATE_ONE, REGION_EU_INVALID)

        val analysisResultsAsDataFrame = AnalysisResult
            .getSuccessMetricsAsDataFrame(session, AnalysisResult(resultKey, results))

        import session.implicits._

        val expected = Seq(
          ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU"),
          ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"),
          ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"))
          .toDF("entity", "instance", "name", "value", "dataset_date", "region")

        assertSameRows(analysisResultsAsDataFrame, expected)
      }
    }

    "turn tagNames into valid columnNames in returned Json" in withSparkSession { session =>

      evaluate(session) { results =>

        val resultKey = ResultKey(DATE_ONE, REGION_EU_INVALID)

        val analysisResultsAsJson = AnalysisResult
            .getSuccessMetricsAsJson(AnalysisResult(resultKey, results))

        val expected =
          s"""[{"entity":"Dataset","instance":"*","name":"Size","value":4.0,
            |"region":"EU", "dataset_date":$DATE_ONE},
            |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0,
            |"region":"EU", "dataset_date":$DATE_ONE},
            |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0,
            |"region":"EU", "dataset_date":$DATE_ONE},
            |{"entity":"Mutlicolumn","instance":"att1,att2",
            |"name":"Uniqueness","value":0.25,
            |"region":"EU", "dataset_date":$DATE_ONE}]"""
            .stripMargin.replaceAll("\n", "")

        assertSameJson(analysisResultsAsJson, expected)
      }
    }

    "avoid duplicate columnNames in returned DataFrame" in withSparkSession { session =>

      evaluate(session) { results =>

        val resultKey = ResultKey(DATE_ONE, DUPLICATE_COLUMN_NAME)

        val analysisResultsAsDataFrame = AnalysisResult
            .getSuccessMetricsAsDataFrame(session, AnalysisResult(resultKey, results))

        import session.implicits._

        val expected = Seq(
          ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU"),
          ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"),
          ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"))
          .toDF("entity", "instance", "name", "value", "dataset_date", "name_2")

        assertSameRows(analysisResultsAsDataFrame, expected)
      }
    }

    "avoid duplicate columnNames in returned Json" in withSparkSession { session =>

      evaluate(session) { results =>

        val resultKey = ResultKey(DATE_ONE, DUPLICATE_COLUMN_NAME)

        val analysisResultsAsJson = AnalysisResult
            .getSuccessMetricsAsJson(AnalysisResult(resultKey, results))

        val expected =
          s"""[{"entity":"Dataset","instance":"*","name":"Size","value":4.0,
            |"name_2":"EU", "dataset_date":$DATE_ONE},
            |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0,
            |"name_2":"EU", "dataset_date":$DATE_ONE},
            |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0,
            |"name_2":"EU", "dataset_date":$DATE_ONE},
            |{"entity":"Mutlicolumn","instance":"att1,att2",
            |"name":"Uniqueness","value":0.25,
            |"name_2":"EU", "dataset_date":$DATE_ONE}]"""
            .stripMargin.replaceAll("\n", "")

        assertSameJson(analysisResultsAsJson, expected)
      }
    }

    "only include some specific tags in returned DataFrame if requested" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val resultKey = ResultKey(DATE_ONE, MULTIPLE_TAGS)

          val analysisResultsAsDataFrame = AnalysisResult
              .getSuccessMetricsAsDataFrame(session, AnalysisResult(resultKey, results),
                withTags = Seq("Region"))

          import session.implicits._

          val expected = Seq(
            ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU"),
            ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"),
            ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"))
            .toDF("entity", "instance", "name", "value", "dataset_date", "region")

          assertSameRows(analysisResultsAsDataFrame, expected)
        }
    }

    "only include some specific tags in returned Json if requested" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val resultKey = ResultKey(DATE_ONE, MULTIPLE_TAGS)

          val analysisResultsAsJson = AnalysisResult
              .getSuccessMetricsAsJson(AnalysisResult(resultKey, results),
                withTags = Seq("Region"))

          val expected =
            s"""[{"entity":"Dataset","instance":"*","name":"Size","value":4.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0,
              |"region":"EU", "dataset_date":$DATE_ONE},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25,
              |"region":"EU", "dataset_date":$DATE_ONE}]"""
              .stripMargin.replaceAll("\n", "")

          assertSameJson(analysisResultsAsJson, expected)
        }
      }

    "return empty DataFrame if AnalyzerContext contains no entries" in withSparkSession { session =>

      val data = getDfFull(session)

      val results = Analysis().run(data)

      val resultKey = ResultKey(DATE_ONE, REGION_EU)

      val analysisResultsAsDataFrame = AnalysisResult
          .getSuccessMetricsAsDataFrame(session, AnalysisResult(resultKey, results))

      import session.implicits._

      val expected = Seq.empty[(String, String, String, Double, Long, String)]
        .toDF("entity", "instance", "name", "value", "dataset_date", "region")

      assertSameRows(analysisResultsAsDataFrame, expected)
    }

    "return empty Json Array if AnalyzerContext contains no entries" in
      withSparkSession { session =>

        val data = getDfFull(session)

        val results = Analysis().run(data)

        val resultKey = ResultKey(DATE_ONE, REGION_EU)

        val analysisResultsAsJson = AnalysisResult
            .getSuccessMetricsAsJson(AnalysisResult(resultKey, results))

        val expected = """[]"""

        assertSameJson(analysisResultsAsJson, expected)
      }
  }

  private[this] def evaluate(session: SparkSession)(test: AnalyzerContext => Unit)
  : Unit = {

    val data = getDfFull(session)

    val results = createAnalysis().run(data)

    test(results)
  }

  private[this] def createAnalysis(): Analysis = {
    Analysis()
      .addAnalyzer(Size())
      .addAnalyzer(Distinctness("item"))
      .addAnalyzer(Completeness("att1"))
      .addAnalyzer(Uniqueness(Seq("att1", "att2")))
  }

  private[this] def createDate(year: Int, month: Int, day: Int): Long = {
    LocalDate.of(year, month, day).atTime(10, 10, 10).toEpochSecond(ZoneOffset.UTC)
  }

  private[this] def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit = {
    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)
  }

  private[this] def assertSameJson(jsonA: String, jsonB: String): Unit = {
    assert(SimpleResultSerde.deserialize(jsonA) ==
      SimpleResultSerde.deserialize(jsonB))
  }
}
