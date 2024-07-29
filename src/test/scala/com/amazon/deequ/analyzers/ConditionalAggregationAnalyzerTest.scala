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

import com.amazon.deequ.metrics.AttributeDoubleMetric

class ConditionalAggregationAnalyzerTest
  extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "ConditionalAggregationAnalyzerTest" should {

    """Example use: return correct counts
      |for product sales in different categories""".stripMargin in withSparkSession
    { session =>
      val data = getDfWithIdColumn(session)
      val mockLambda: DataFrame => AggregatedMetricState = _ =>
        AggregatedMetricState(Map("ProductA" -> 50, "ProductB" -> 45), 100)

      val analyzer = ConditionalAggregationAnalyzer(mockLambda, "ProductSales", "category")

      val state = analyzer.computeStateFrom(data)
      val metric: AttributeDoubleMetric = analyzer.computeMetricFrom(state)

      metric.value.isSuccess shouldBe true
      metric.value.get should contain ("ProductA" -> 0.5)
      metric.value.get should contain ("ProductB" -> 0.45)
    }

    "handle scenarios with no data points effectively" in withSparkSession { session =>
      val data = getDfWithIdColumn(session)
      val mockLambda: DataFrame => AggregatedMetricState = _ =>
        AggregatedMetricState(Map.empty[String, Int], 100)

      val analyzer = ConditionalAggregationAnalyzer(mockLambda, "WebsiteTraffic", "page")

      val state = analyzer.computeStateFrom(data)
      val metric: AttributeDoubleMetric = analyzer.computeMetricFrom(state)

      metric.value.isSuccess shouldBe true
      metric.value.get shouldBe empty
    }

    "return a failure metric when the lambda function fails" in withSparkSession { session =>
      val data = getDfWithIdColumn(session)
      val failingLambda: DataFrame => AggregatedMetricState =
        _ => throw new RuntimeException("Test failure")

      val analyzer = ConditionalAggregationAnalyzer(failingLambda, "ProductSales", "category")

      val state = analyzer.computeStateFrom(data)
      val metric = analyzer.computeMetricFrom(state)

      metric.value.isFailure shouldBe true
      metric.value match {
        case Success(_) => fail("Should have failed due to lambda function failure")
        case Failure(exception) => exception.getMessage shouldBe "Metric computation failed"
      }
    }

    "return a failure metric if there are no rows in DataFrame" in withSparkSession { session =>
      val emptyData = session.createDataFrame(
        session.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        getDfWithIdColumn(session).schema)
      val mockLambda: DataFrame => AggregatedMetricState = df =>
        if (df.isEmpty) throw new RuntimeException("No data to analyze")
        else AggregatedMetricState(Map("ProductA" -> 0, "ProductB" -> 0), 0)

      val analyzer = ConditionalAggregationAnalyzer(mockLambda,
        "ProductSales",
        "category")

      val state = analyzer.computeStateFrom(emptyData)
      val metric = analyzer.computeMetricFrom(state)

      metric.value.isFailure shouldBe true
      metric.value match {
        case Success(_) => fail("Should have failed due to no data")
        case Failure(exception) => exception.getMessage should include("Metric computation failed")
      }
    }
  }

  def getDfWithIdColumn(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      ("ProductA", "North"),
      ("ProductA", "South"),
      ("ProductB", "East"),
      ("ProductA", "West")
    ).toDF("product", "region")
  }
}
