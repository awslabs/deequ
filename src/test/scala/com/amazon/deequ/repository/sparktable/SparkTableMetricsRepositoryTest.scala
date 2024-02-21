/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.repository.sparktable

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class SparkTableMetricsRepositoryTest extends AnyWordSpec
  with SparkContextSpec
  with FixtureSupport {

  // private var spark: SparkSession = _
  //  private var repository: SparkTableMetricsRepository = _
  private val analyzer = Size()

  "spark table metrics repository " should {
    "save and load a single metric" in withSparkSessionCustomWareHouse { spark =>
      val resultKey = ResultKey(System.currentTimeMillis(), Map("tag" -> "value"))
      val metric = DoubleMetric(Entity.Column, "m1", "", Try(100))
      val context = AnalyzerContext(Map(analyzer -> metric))

      val repository = new SparkTableMetricsRepository(spark, "metrics_table")
      // Save the metric
      repository.save(resultKey, context)

      // Load the metric
      val loadedContext = repository.loadByKey(resultKey)

      assert(loadedContext.isDefined)
      assert(loadedContext.get.metric(analyzer).contains(metric))

    }

    "save multiple metrics and load them" in withSparkSessionCustomWareHouse { spark =>
      val repository = new SparkTableMetricsRepository(spark, "metrics_table")

      val resultKey1 = ResultKey(System.currentTimeMillis(), Map("tag" -> "tagValue1"))
      val metric = DoubleMetric(Entity.Column, "m1", "", Try(100))
      val context1 = AnalyzerContext(Map(analyzer -> metric))

      val resultKey2 = ResultKey(System.currentTimeMillis(), Map("tag" -> "tagValue2"))
      val metric2 = DoubleMetric(Entity.Column, "m2", "", Try(101))
      val context2 = AnalyzerContext(Map(analyzer -> metric2))

      repository.save(resultKey1, context1)
      repository.save(resultKey2, context2)

      val loadedMetrics = repository.load().get()

      assert(loadedMetrics.length == 2)

      loadedMetrics.flatMap(_.resultKey.tags)

    }

    "save and load metrics with tag" in withSparkSessionCustomWareHouse { spark =>
      val repository = new SparkTableMetricsRepository(spark, "metrics_table")

      val resultKey1 = ResultKey(System.currentTimeMillis(), Map("tag" -> "A"))
      val metric = DoubleMetric(Entity.Column, "m1", "", Try(100))
      val context1 = AnalyzerContext(Map(analyzer -> metric))

      val resultKey2 = ResultKey(System.currentTimeMillis(), Map("tag" -> "B"))
      val metric2 = DoubleMetric(Entity.Column, "m2", "", Try(101))
      val context2 = AnalyzerContext(Map(analyzer -> metric2))

      repository.save(resultKey1, context1)
      repository.save(resultKey2, context2)
      val loadedMetricsForTagA = repository.load().withTagValues(Map("tag" -> "A")).get()
      assert(loadedMetricsForTagA.length == 1)

      val tagsMapA = loadedMetricsForTagA.flatMap(_.resultKey.tags).toMap
      assert(tagsMapA.size == 1, "should have 1 result")
      assert(tagsMapA.contains("tag"), "should contain tag")
      assert(tagsMapA("tag") == "A", "tag should be A")

      val loadedMetricsForAllMetrics = repository.load().forAnalyzers(Seq(analyzer)).get()
      assert(loadedMetricsForAllMetrics.length == 2, "should have 2 results")

    }

    "save and load to iceberg a single metric" in withSparkSessionIcebergCatalog { spark => {
      // The SupportsRowLevelOperations class is available from spark 3.3
      // We should skip this test for lower spark versions
      val className = "org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations"
      if (Try(Class.forName(className)).isSuccess) {
        val resultKey = ResultKey(System.currentTimeMillis(), Map("tag" -> "value"))
        val metric = DoubleMetric(Entity.Column, "m1", "", Try(100))
        val context = AnalyzerContext(Map(analyzer -> metric))

        val repository = new SparkTableMetricsRepository(spark, "local.metrics_table")
        // Save the metric
        repository.save(resultKey, context)

        // Load the metric
        val loadedContext = repository.loadByKey(resultKey)

        assert(loadedContext.isDefined)
        assert(loadedContext.get.metric(analyzer).contains(metric))
      }
    } }
  }
}
