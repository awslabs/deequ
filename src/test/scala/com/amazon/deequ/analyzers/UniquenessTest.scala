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

package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{Matchers, WordSpec}

class UniquenessTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  def uniquenessSampleData(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    // Example from https://github.com/awslabs/deequ/issues/178
    Seq(
      ("India", "Xavier House, 2nd Floor", "St. Peter Colony, Perry Road", "Bandra (West)"),
      ("India", "503 Godavari", "Sir Pochkhanwala Road", "Worli"),
      ("India", "4/4 Seema Society", "N Dutta Road, Four Bungalows", "Andheri"),
      ("India", "1001D Abhishek Apartments", "Juhu Versova Road", "Andheri"),
      ("India", "95, Hill Road", null, null),
      ("India", "90 Cuffe Parade", "Taj President Hotel", "Cuffe Parade"),
      ("India", "4, Seven PM", "Sir Pochkhanwala Rd", "Worli"),
      ("India", "1453 Sahar Road", null, null)
    )
      .toDF("Country", "Address Line 1", "Address Line 2", "Address Line 3")
  }

  "Uniqueness" should {

    "be correct for multiple fields" in withSparkSession { session =>

      val data = uniquenessSampleData(session)

      val stateStore = InMemoryStateProvider()

      val uniquenessA1 = Uniqueness("Address Line 1")
      val uniquenessA13 = Uniqueness(Seq("Address Line 1", "Address Line 2", "Address Line 3"))

      val analysis = Analysis(Seq(uniquenessA1, uniquenessA13))

      val result = AnalysisRunner.run(data, analysis, saveStatesWith = Some(stateStore))

      assert(result.metric(uniquenessA1).get.asInstanceOf[DoubleMetric].value.get == 1.0)
      assert(result.metric(uniquenessA13).get.asInstanceOf[DoubleMetric].value.get == 1.0)
    }
  }

  "Filtered Uniqueness" in withSparkSession { sparkSession =>
    import sparkSession.implicits._
    val df = Seq(
      ("1", "unique"),
      ("2", "unique"),
      ("3", "duplicate"),
      ("3", "duplicate"),
      ("4", "unique")
    ).toDF("value", "type")

    val stateStore = InMemoryStateProvider()

    val uniqueness = Uniqueness("value")
    val uniquenessWithFilter = Uniqueness(Seq("value"), Some("type = 'unique'"))

    val analysis = Analysis(Seq(uniqueness, uniquenessWithFilter))

    val result = AnalysisRunner.run(df, analysis, saveStatesWith = Some(stateStore))

    assert(result.metric(uniqueness).get.asInstanceOf[DoubleMetric].value.get == 0.6)
    assert(result.metric(uniquenessWithFilter).get.asInstanceOf[DoubleMetric].value.get == 1.0)
  }
}
