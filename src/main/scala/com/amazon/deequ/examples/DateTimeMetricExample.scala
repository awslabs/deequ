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

import java.time.Instant
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.{DateTimeDistribution, DistributionInterval, MaximumDateTime, MinimumDateTime}
import com.amazon.deequ.examples.ExampleUtils.{customerAsDataframe, withSpark}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame

private[examples] object DateTimeMetricExample extends App {
  withSpark { session =>

    val data = customerAsDataframe(session,
      Customer(1, "john doe", Instant.parse("2021-11-11T07:15:00Z")),
      Customer(2, "marry jane", Instant.parse("2022-01-11T07:45:00Z")),
      Customer(3, "Thomas Yu", Instant.parse("2023-02-11T08:15:00Z")),
      Customer(4, "Steve Powell", Instant.parse("2019-04-11T12:15:00Z")),
      Customer(5, "Andrej Kar", Instant.parse("2020-08-11T12:30:00Z")),
    )

    val analysisResult: AnalyzerContext = { AnalysisRunner
      .onData(data)
      .addAnalyzer(DateTimeDistribution("dateOfBirth", DistributionInterval.HOURLY))
//      .addAnalyzer(MinimumDateTime("dateOfBirth"))
//      .addAnalyzer(MaximumDateTime("dateOfBirth"))
      .run()
    }

    successMetricsAsDataFrame(session, analysisResult).show(false)

    analysisResult.metricMap.foreach( x =>
      println(s"column '${x._2.instance}' has ${x._2.name} : ${x._2.value.get}")
    )

  }
}
