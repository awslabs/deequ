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

import com.amazon.deequ.examples.ExampleUtils.withSpark
import com.amazon.deequ.profiles.{ColumnProfilerRunner, NumericColumnProfile}

case class RawData(productName: String, totalNumber: String, status: String, valuable: String)

private[examples] object DataProfilingExample extends App {

  withSpark { session =>

    /* We profile raw data, mostly in string format (e.g., from a csv file) */
    val rows = session.sparkContext.parallelize(Seq(
      RawData("thingA", "13.0", "IN_TRANSIT", "true"),
      RawData("thingA", "5", "DELAYED", "false"),
      RawData("thingB", null, "DELAYED", null),
      RawData("thingC", null, "IN_TRANSIT", "false"),
      RawData("thingD", "1.0", "DELAYED", "true"),
      RawData("thingC", "7.0", "UNKNOWN", null),
      RawData("thingC", "20", "UNKNOWN", null),
      RawData("thingE", "20", "DELAYED", "false")
    ))

    val rawData = session.createDataFrame(rows)

    /* Make deequ profile this data. It will execute the three passes over the data and avoid
       any shuffles. */
    val result = ColumnProfilerRunner()
      .onData(rawData)
      .run()

    /* We get a profile for each column which allows to inspect the completeness of the column,
       the approximate number of distinct values and the inferred datatype. */
    result.profiles.foreach { case (productName, profile) =>

      println(s"Column '$productName':\n " +
        s"\tcompleteness: ${profile.completeness}\n" +
        s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
        s"\tdatatype: ${profile.dataType}\n")
    }

    /* For numeric columns, we get descriptive statistics */
    val totalNumberProfile = result.profiles("totalNumber").asInstanceOf[NumericColumnProfile]

    println(s"Statistics of 'totalNumber':\n" +
      s"\tminimum: ${totalNumberProfile.minimum.get}\n" +
      s"\tmaximum: ${totalNumberProfile.maximum.get}\n" +
      s"\tmean: ${totalNumberProfile.mean.get}\n" +
      s"\tstandard deviation: ${totalNumberProfile.stdDev.get}\n")

    val statusProfile = result.profiles("status")

    /* For columns with a low number of distinct values, we get the full value distribution. */
    println("Value distribution in 'stats':")
    statusProfile.histogram.foreach {
      _.values.foreach { case (key, entry) =>
        println(s"\t$key occurred ${entry.absolute} times (ratio is ${entry.ratio})")
      }
    }

  }
}
