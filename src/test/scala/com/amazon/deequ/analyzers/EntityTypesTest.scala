/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Failure
import scala.util.Success

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import com.amazon.deequ.metrics.{Entity, EntityKeyedDoubleMetric}

class EntityDetectionAnalyzerTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "EntityDetectionAnalyzer" should {

    "return a metric with correct detection rates when entities are detected accurately" in withSparkSession
    { session =>
      val data = getDfWithIdColumn(session)
      val mockLambda: DataFrame => EntityDetectionState = _ =>
        EntityDetectionState(Map("SSN" -> 50, "EMAIL" -> 45), 100)

      val analyzer = EntityDetectionAnalyzer(mockLambda, "id")

      val state = analyzer.computeStateFrom(data)
      val metric: EntityKeyedDoubleMetric = analyzer.computeMetricFrom(state)

      metric.value.isSuccess shouldBe true
      metric.value.get should contain ("SSN" -> 0.5)
      metric.value.get should contain ("EMAIL" -> 0.45)
    }

    "handle no detected entities" in withSparkSession { session =>
      val data = getDfWithIdColumn(session)
      val mockLambda: DataFrame => EntityDetectionState = _ =>
        EntityDetectionState(Map.empty[String, Int], 100)

      val analyzer = EntityDetectionAnalyzer(mockLambda, "id")

      val state = analyzer.computeStateFrom(data)
      val metric: EntityKeyedDoubleMetric = analyzer.computeMetricFrom(state)

      metric.value.isSuccess shouldBe true
      metric.value.get shouldBe empty
    }

    "return a failure metric when the lambda function fails" in withSparkSession { session =>
      val data = getDfWithIdColumn(session)
      val failingLambda: DataFrame => EntityDetectionState = _ => throw new RuntimeException("Test failure")

      val analyzer = EntityDetectionAnalyzer(failingLambda, "id")

      val state = analyzer.computeStateFrom(data)
      val metric = analyzer.computeMetricFrom(state)

      metric.value.isFailure shouldBe true
      metric.value match {
        case Success(_) => fail("Should have failed due to lambda function failure")
        case Failure(exception) => exception.getMessage shouldBe "Entity detection failed"
      }
    }

    "return a failure metric if there are no rows in DataFrame" in withSparkSession { session =>
      val emptyData = session.createDataFrame(session.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        getDfWithIdColumn(session).schema)
      val mockLambda: DataFrame => EntityDetectionState = df =>
        if (df.isEmpty) throw new RuntimeException("No data to analyze") // Explicitly fail if the data is empty
        else EntityDetectionState(Map("SSN" -> 0, "EMAIL" -> 0), 0)

      val analyzer = EntityDetectionAnalyzer(mockLambda, "id")

      val state = analyzer.computeStateFrom(emptyData)
      val metric = analyzer.computeMetricFrom(state)

      metric.value.isFailure shouldBe true
      metric.value match {
        case Success(_) => fail("Should have failed due to no data")
        case Failure(exception) => exception.getMessage should include("Entity detection failed")
      }
    }
  }

  def getDfWithIdColumn(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      (1, "123-45-6789"),
      (2, "987-65-4321"),
      (3, "111-22-3333")
    ).toDF("id", "SSN")
  }
}
