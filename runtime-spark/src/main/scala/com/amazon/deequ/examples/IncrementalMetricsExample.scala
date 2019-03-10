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

package com.amazon.deequ.examples

import ExampleUtils.{itemsAsDataframe, withSpark}
import com.amazon.deequ.Analysis
import com.amazon.deequ.runtime.spark.{InMemorySparkStateProvider, SparkDataset, SparkEngine}
import com.amazon.deequ.statistics.{ApproxCountDistinct, Completeness, Size}
import org.apache.spark.sql.SparkSession


private[examples] object IncrementalMetricsExample extends App {

  /* NOTE: Stateful support is still work in progress, and is therefore not yet integrated into
     VerificationSuite. We showcase however how to incrementally compute metrics on a growing
     dataset using the AnalysisRunner. */

  withSpark { session =>

    val engine = SparkEngine(session)

    val data = itemsAsDataframe(session,
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available tomorrow", "low", 0),
      Item(3, "Thing C", null, null, 5))

    val moreData = itemsAsDataframe(session,
      Item(4, "Thingy D", null, "low", 10),
      Item(5, "Thingy E", null, "high", 12))

    val statistics = Seq(Size(), ApproxCountDistinct("id"), Completeness("name"), Completeness("description"))

    val stateStore = InMemorySparkStateProvider()

    val metricsForData = Analysis
      .onData(SparkDataset(data), engine)
      .addAnalyzers(statistics)
      .saveStatesWith(stateStore)
      .run()

    // We update the metrics now from the stored states without having to access the previous data!
    val metricsAfterAddingMoreData = Analysis
      .onData(SparkDataset(moreData), engine)
      .addAnalyzers(statistics)
      .aggregateWith(stateStore)
      .run()

    println("Metrics for the first 3 records:\n")
    metricsForData.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }

    println("\nMetrics after adding 2 more records:\n")
    metricsAfterAddingMoreData.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }

  }
}
