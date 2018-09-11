package com.amazon.deequ.analyzers.runners

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.repository.SimpleResultSerde
import org.apache.spark.sql.{DataFrame, SparkSession}

class AnalyzerContextTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "AnalyzerContext" should {
    "correctly return a DataFrame that is formatted as expected" in withSparkSession { session =>

      evaluate(session) { results =>

        val successMetricsAsDataFrame = AnalyzerContext.successMetricsAsDataFrame(session, results)

        import session.implicits._
        val expected = Seq(
          ("Dataset", "*", "Size", 4.0),
          ("Column", "item", "Distinctness", 1.0),
          ("Column", "att1", "Completeness", 1.0),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25))
            .toDF("entity", "instance", "name", "value")

        assertSameRows(successMetricsAsDataFrame, expected)
      }
    }

    "only include specific metrics in returned DataFrame if requested" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val metricsForAnalyzers = Seq(Completeness("att1"), Uniqueness(Seq("att1", "att2")))

          val successMetricsAsDataFrame = AnalyzerContext
              .successMetricsAsDataFrame(session, results, metricsForAnalyzers)

          import session.implicits._
          val expected = Seq(
            ("Column", "att1", "Completeness", 1.0),
            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25))
            .toDF("entity", "instance", "name", "value")

          assertSameRows(successMetricsAsDataFrame, expected)
        }
    }

    "correctly return Json that is formatted as expected" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val successMetricsResultsJson = AnalyzerContext.successMetricsAsJson(results)

          val expectedJson =
            """[{"entity":"Dataset","instance":"*","name":"Size","value":4.0},
              |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0},
              |{"entity":"Column","instance":"item","name":"Distinctness","value":1.0},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25}]"""
              .stripMargin.replaceAll("\n", "")

          assertSameJson(successMetricsResultsJson, expectedJson)
        }
      }

    "only include requested metrics in returned Json" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val metricsForAnalyzers = Seq(Completeness("att1"), Uniqueness(Seq("att1", "att2")))

          val successMetricsResultsJson =
            AnalyzerContext.successMetricsAsJson(results, metricsForAnalyzers)

          val expectedJson =
            """[{"entity":"Column","instance":"att1","name":"Completeness","value":1.0},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25}]"""
            .stripMargin.replaceAll("\n", "")

          assertSameJson(successMetricsResultsJson, expectedJson)
        }
      }
  }

  private[this] def evaluate(session: SparkSession)(test: AnalyzerContext => Unit): Unit = {

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

  private[this] def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit = {
    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)
  }

  private[this] def assertSameJson(jsonA: String, jsonB: String): Unit = {
    assert(SimpleResultSerde.deserialize(jsonA) ==
      SimpleResultSerde.deserialize(jsonB))
  }
}
