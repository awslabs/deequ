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

package com.amazon.deequ

import com.amazon.deequ.analyzers._
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.SimpleResultSerde
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{Matchers, WordSpec}

class VerificationResultTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "VerificationResult getSuccessMetrics" should {

    "correctly return a DataFrame that is formatted as expected" in withSparkSession { session =>

      evaluate(session) { results =>

        val successMetricsAsDataFrame = VerificationResult
          .successMetricsAsDataFrame(session, results)

        import session.implicits._
        val expected = Seq(
          ("Dataset", "*", "Size", 4.0),
          ("Column", "att2", "Completeness", 1.0),
          ("Column", "item", "Distinctness", 1.0),
          ("Column", "att1", "Completeness", 1.0),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25)
        )
          .toDF("entity", "instance", "name", "value")

        assertSameRows(successMetricsAsDataFrame, expected)
      }
    }

    "only include specific metrics in returned DataFrame if requested" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val metricsForAnalyzers = Seq(Completeness("att1"), Uniqueness(Seq("att1", "att2")))

          val successMetricsAsDataFrame = VerificationResult
            .successMetricsAsDataFrame(session, results, metricsForAnalyzers)

          import session.implicits._
          val expected = Seq(
            ("Column", "att1", "Completeness", 1.0),
            ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25)
          )
            .toDF("entity", "instance", "name", "value")

          assertSameRows(successMetricsAsDataFrame, expected)
        }
    }

    "correctly return Json that is formatted as expected" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val successMetricsResultsJson = VerificationResult.successMetricsAsJson(results)

          val expectedJson =
            """[{"entity":"Column","instance":"item","name":"Distinctness","value":1.0},
              |{"entity": "Column", "instance":"att2","name":"Completeness","value":1.0},
              |{"entity":"Column","instance":"att1","name":"Completeness","value":1.0},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25},
              |{"entity":"Dataset","instance":"*","name":"Size","value":4.0}]"""
              .stripMargin.replaceAll("\n", "")

          assertSameResultsJson(successMetricsResultsJson, expectedJson)
        }
      }

    "only include requested metrics in returned Json" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val metricsForAnalyzers = Seq(Completeness("att1"), Uniqueness(Seq("att1", "att2")))

          val successMetricsResultsJson =
            VerificationResult.successMetricsAsJson(results, metricsForAnalyzers)

          val expectedJson =
            """[{"entity":"Column","instance":"att1","name":"Completeness","value":1.0},
              |{"entity":"Mutlicolumn","instance":"att1,att2",
              |"name":"Uniqueness","value":0.25}]"""
              .stripMargin.replaceAll("\n", "")

           assertSameResultsJson(successMetricsResultsJson, expectedJson)
        }
      }
  }

   "VerificationResult getCheckResults" should {

    "correctly return a DataFrame that is formatted as expected" in withSparkSession { session =>

      evaluate(session) { results =>

        val successMetricsAsDataFrame = VerificationResult
          .checkResultsAsDataFrame(session, results)

        import session.implicits._
        val expected = Seq(
          ("group-1", "Error", "Success", "CompletenessConstraint(Completeness(att1,None))",
            "Success", ""),
          ("group-2-E", "Error", "Error", "SizeConstraint(Size(None))", "Failure",
            "Value: 4 does not meet the constraint requirement! Should be greater than 5!"),
          ("group-2-E", "Error", "Error", "CompletenessConstraint(Completeness(att2,None))",
            "Success", ""),
          ("group-2-W", "Warning", "Warning",
            "DistinctnessConstraint(Distinctness(List(item),None))",
            "Failure", "Value: 1.0 does not meet the constraint requirement! " +
            "Should be smaller than 0.8!")
        )
          .toDF("check", "check_level", "check_status", "constraint",
            "constraint_status", "constraint_message")

        assertSameRows(successMetricsAsDataFrame, expected)
      }
    }

    "correctly return Json that is formatted as expected" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val checkResultsAsJson = VerificationResult.checkResultsAsJson(results)

          val expectedJson =
            """[{"check":"group-1","check_level":"Error","check_status":"Success",
              |"constraint":"CompletenessConstraint(Completeness(att1,None))",
              |"constraint_status":"Success","constraint_message":""},
              |
              |{"check":"group-2-E","check_level":"Error","check_status":"Error",
              |"constraint":"SizeConstraint(Size(None))", "constraint_status":"Failure",
              |"constraint_message":"Value: 4 does not meet the constraint requirement!
              | Should be greater than 5!"},
              |
              |{"check":"group-2-E","check_level":"Error","check_status":"Error",
              |"constraint":"CompletenessConstraint(Completeness(att2,None))",
              |"constraint_status":"Success","constraint_message":""},
              |
              |{"check":"group-2-W","check_level":"Warning","check_status":"Warning",
              |"constraint":"DistinctnessConstraint(Distinctness(List(item),None))",
              |"constraint_status":"Failure",
              |"constraint_message":"Value: 1.0 does not meet the constraint requirement!
              | Should be smaller than 0.8!"}]"""
              .stripMargin.replaceAll("\n", "")

          assertSameResultsJson(checkResultsAsJson, expectedJson)
        }
      }
  }

  private[this] def evaluate(session: SparkSession)(test: VerificationResult => Unit): Unit = {

    val data = getDfFull(session)

    val analyzers = getAnalyzers()
    val checks = getChecks()

    val results = VerificationSuite()
      .onData(data)
      .addRequiredAnalyzers(analyzers)
      .addChecks(checks)
      .run()

    test(results)
  }

  private[this] def getAnalyzers(): Seq[Analyzer[_, Metric[_]]] = {
      Uniqueness(Seq("att1", "att2")) ::
      Nil
  }

  private[this] def getChecks(): Seq[Check] = {
    val checkToSucceed = Check(CheckLevel.Error, "group-1")
      .isComplete("att1")

    val checkToErrorOut = Check(CheckLevel.Error, "group-2-E")
      .hasSize(_ > 5, Some("Should be greater than 5!"))
      .hasCompleteness("att2", _ == 1.0, Some("Should equal 1!"))

    val checkToWarn = Check(CheckLevel.Warning, "group-2-W")
      .hasDistinctness(Seq("item"), _ < 0.8, Some("Should be smaller than 0.8!"))

    checkToSucceed :: checkToErrorOut :: checkToWarn :: Nil
  }

  private[this] def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit = {
    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)
  }

  private[this] def assertSameResultsJson(jsonA: String, jsonB: String): Unit = {
    assert(SimpleResultSerde.deserialize(jsonA) ==
      SimpleResultSerde.deserialize(jsonB))
  }
}
