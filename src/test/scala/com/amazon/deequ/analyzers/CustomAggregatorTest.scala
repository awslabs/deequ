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
import com.amazon.deequ.analyzers._
import com.amazon.deequ.metrics.AttributeDoubleMetric
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, count}
import scala.util.Failure
import scala.util.Success
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import com.amazon.deequ.metrics.AttributeDoubleMetric
import com.amazon.deequ.profiles.NumericColumnProfile

class CustomAggregatorTest
  extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "CustomAggregatorTest" should {

    """Example use: return correct counts
      |for product sales in different categories""".stripMargin in withSparkSession
    { session =>
      val data = getDfWithIdColumn(session)
      val mockLambda: DataFrame => AggregatedMetricState = _ =>
        AggregatedMetricState(Map("ProductA" -> 50, "ProductB" -> 45), 100)

      val analyzer = CustomAggregator(mockLambda, "ProductSales", "category")

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

      val analyzer = CustomAggregator(mockLambda, "WebsiteTraffic", "page")

      val state = analyzer.computeStateFrom(data)
      val metric: AttributeDoubleMetric = analyzer.computeMetricFrom(state)

      metric.value.isSuccess shouldBe true
      metric.value.get shouldBe empty
    }

    "return a failure metric when the lambda function fails" in withSparkSession { session =>
      val data = getDfWithIdColumn(session)
      val failingLambda: DataFrame => AggregatedMetricState =
        _ => throw new RuntimeException("Test failure")

      val analyzer = CustomAggregator(failingLambda, "ProductSales", "category")

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

      val analyzer = CustomAggregator(mockLambda,
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

  "Combined Analysis with CustomAggregator and ColumnProfilerRunner" should {
    "provide aggregated data and column profiles" in withSparkSession { session =>
      import session.implicits._

      // Define the dataset
      val rawData = Seq(
        ("thingA", "13.0", "IN_TRANSIT", "true"),
        ("thingA", "5", "DELAYED", "false"),
        ("thingB", null, "DELAYED", null),
        ("thingC", null, "IN_TRANSIT", "false"),
        ("thingD", "1.0", "DELAYED", "true"),
        ("thingC", "7.0", "UNKNOWN", null),
        ("thingC", "20", "UNKNOWN", null),
        ("thingE", "20", "DELAYED", "false")
      ).toDF("productName", "totalNumber", "status", "valuable")

      val statusCountLambda: DataFrame => AggregatedMetricState = df =>
        AggregatedMetricState(df.groupBy("status").count().rdd
          .map(r => r.getString(0) -> r.getLong(1).toInt).collect().toMap, df.count().toInt)

      val statusAnalyzer = CustomAggregator(statusCountLambda, "ProductStatus")
      val statusMetric = statusAnalyzer.computeMetricFrom(statusAnalyzer.computeStateFrom(rawData))

      val result = ColumnProfilerRunner().onData(rawData).run()

      statusMetric.value.isSuccess shouldBe true
      statusMetric.value.get("IN_TRANSIT") shouldBe 0.25
      statusMetric.value.get("DELAYED") shouldBe 0.5

      val totalNumberProfile = result.profiles("totalNumber").asInstanceOf[NumericColumnProfile]
      totalNumberProfile.completeness shouldBe 0.75
      totalNumberProfile.dataType shouldBe DataTypeInstances.Fractional

      result.profiles.foreach { case (colName, profile) =>
        println(s"Column '$colName': completeness: ${profile.completeness}, " +
          s"approximate number of distinct values: ${profile.approximateNumDistinctValues}")
      }
    }
  }

  "accurately compute percentage occurrences and total engagements for content types" in withSparkSession { session =>
    val data = getContentEngagementDataFrame(session)
    val contentEngagementLambda: DataFrame => AggregatedMetricState = df => {

      // Calculate the total engagements for each content type
      val counts = df
        .groupBy("content_type")
        .agg(
          (sum("views") + sum("likes") + sum("shares")).cast("int").alias("totalEngagements")
        )
        .collect()
        .map(row =>
          row.getString(0) -> row.getInt(1)
        )
        .toMap
      val totalEngagements = counts.values.sum
      AggregatedMetricState(counts, totalEngagements)
    }

    val analyzer = CustomAggregator(contentEngagementLambda, "ContentEngagement", "AllTypes")

    val state = analyzer.computeStateFrom(data)
    val metric = analyzer.computeMetricFrom(state)

    metric.value.isSuccess shouldBe true
    //  Counts: Map(Video -> 5300, Article -> 1170)
    //  total engagement: 6470
    (metric.value.get("Video") * 100).toInt shouldBe 81
    (metric.value.get("Article") * 100).toInt shouldBe 18
    println(metric.value.get)
  }

  "accurately compute total aggregated resources for cloud services" in withSparkSession { session =>
    val data = getResourceUtilizationDataFrame(session)
    val resourceUtilizationLambda: DataFrame => AggregatedMetricState = df => {
      val counts = df.groupBy("service_type")
        .agg(
          (sum("cpu_hours") + sum("memory_gbs") + sum("storage_gbs")).cast("int").alias("totalResources")
        )
        .collect()
        .map(row =>
          row.getString(0) -> row.getInt(1)
        )
        .toMap
      val totalResources = counts.values.sum
      AggregatedMetricState(counts, totalResources)
    }
    val analyzer = CustomAggregator(resourceUtilizationLambda, "ResourceUtilization", "CloudServices")

    val state = analyzer.computeStateFrom(data)
    val metric = analyzer.computeMetricFrom(state)

    metric.value.isSuccess shouldBe true
    println("Resource Utilization Metrics: " + metric.value.get)
//  Resource Utilization Metrics: Map(Compute -> 0.5076142131979695,
    //                                Database -> 0.27918781725888325,
    //                                Storage -> 0.2131979695431472)
    (metric.value.get("Compute") * 100).toInt shouldBe 50  // Expected percentage for Compute
    (metric.value.get("Database") * 100).toInt shouldBe 27  // Expected percentage for Database
    (metric.value.get("Storage") * 100).toInt shouldBe 21   // 430 CPU + 175 Memory + 140 Storage from mock data
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

  def getContentEngagementDataFrame(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      ("Video", 1000, 150, 300),
      ("Article", 500, 100, 150),
      ("Video", 1500, 200, 450),
      ("Article", 300, 50, 70),
      ("Video", 1200, 180, 320)
    ).toDF("content_type", "views", "likes", "shares")
  }

  def getResourceUtilizationDataFrame(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      ("Compute", 400, 120, 150),
      ("Storage", 100, 30, 500),
      ("Database", 200, 80, 100),
      ("Compute", 450, 130, 250),
      ("Database", 230, 95, 120)
    ).toDF("service_type", "cpu_hours", "memory_gbs", "storage_gbs")
  }
}
