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

import java.io.File

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.examples.ExampleUtils.{itemsAsDataframe, withSpark}
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.google.common.io.Files

object MetricsRepositoryExample extends App {

  withSpark { session =>

    // The toy data on which we will compute metrics
    val data = itemsAsDataframe(session,
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12))

    // A json file in which the computed metrics will be stored
    val metricsFile = new File(Files.createTempDir(), "metrics.json")

    // The repository which we will use to stored and load computed metrics; we use the local disk,
    // but it also supports HDFS and S3
    val repository: MetricsRepository =
      FileSystemMetricsRepository(session, metricsFile.getAbsolutePath)

    // The key under which we store the results, needs a timestamp and supports arbitrary
    // tags in the form of key-value pairs
    val resultKey = ResultKey(System.currentTimeMillis(), Map("tag" -> "repositoryExample"))

    VerificationSuite()
      .onData(data)
      // Some integrity checks
      .addCheck(Check(CheckLevel.Error, "integrity checks")
        .hasSize(_ == 5)
        .isComplete("id")
        .isComplete("productName")
        .isContainedIn("priority", Array("high", "low"))
        .isNonNegative("numViews"))
      // We want to store the computed metrics for the checks in our repository
      .useRepository(repository)
      .saveOrAppendResult(resultKey)
      .run()

    // We can now retrieve the metrics from the repository in different ways, e.g.:


    // We can load the metric for a particular analyzer stored under our result key:
    val completenessOfProductName = repository
      .loadByKey(resultKey).get
      .metric(Completeness("productName")).get

    println(s"The completeness of the productName column is: $completenessOfProductName")

    // We can query the repository for all metrics from the last 10 minutes and get them as json
    val json = repository.load()
      .after(System.currentTimeMillis() - 10000)
      .getSuccessMetricsAsJson()

    println(s"Metrics from the last 10 minutes:\n$json")

    // Finally we can also query by tag value and retrieve the result in the form of a dataframe
    repository.load()
      .withTagValues(Map("tag" -> "repositoryExample"))
      .getSuccessMetricsAsDataFrame(session)
      .show()
  }
}
