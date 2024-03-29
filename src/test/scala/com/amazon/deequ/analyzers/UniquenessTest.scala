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
import com.amazon.deequ.VerificationResult.UNIQUENESS_ID
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.FullColumn
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UniquenessTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

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

  "return row-level results for uniqueness with multiple columns" in withSparkSession { session =>

    val data = getDfWithUniqueColumns(session)

    val addressLength = Uniqueness(Seq("onlyUniqueWithOtherNonUnique", "nonUniqueWithNulls")) // It's null in two rows
    val state: Option[FrequenciesAndNumRows] = addressLength.computeStateFrom(data)
    val metric: DoubleMetric with FullColumn = addressLength.computeMetricFrom(state)

    // Adding column with UNIQUENESS_ID, since it's only added in VerificationResult.getRowLevelResults
    data.withColumn(UNIQUENESS_ID, monotonically_increasing_id())
      .withColumn("new", metric.fullColumn.get).orderBy("unique")
      .collect().map(_.getAs[Boolean]("new")) shouldBe Seq(true, true, true, false, false, false)
  }

  "return row-level results for uniqueness with null" in withSparkSession { session =>

    val data = getDfWithUniqueColumns(session)

    val addressLength = Uniqueness(Seq("uniqueWithNulls"))
    val state: Option[FrequenciesAndNumRows] = addressLength.computeStateFrom(data)
    val metric: DoubleMetric with FullColumn = addressLength.computeMetricFrom(state)

    // Adding column with UNIQUENESS_ID, since it's only added in VerificationResult.getRowLevelResults
    data.withColumn(UNIQUENESS_ID, monotonically_increasing_id())
      .withColumn("new", metric.fullColumn.get).orderBy("unique")
      .collect().map(_.getAs[Boolean]("new")) shouldBe Seq(true, true, true, true, true, true)
  }

  "return filtered row-level results for uniqueness with null" in withSparkSession { session =>

    val data = getDfWithUniqueColumns(session)

    val addressLength = Uniqueness(Seq("onlyUniqueWithOtherNonUnique"), Option("unique < 4"),
      Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)))
    val state: Option[FrequenciesAndNumRows] = addressLength.computeStateFrom(data, Option("unique < 4"))
    val metric: DoubleMetric with FullColumn = addressLength.computeMetricFrom(state)

    // Adding column with UNIQUENESS_ID, since it's only added in VerificationResult.getRowLevelResults
    val resultDf = data.withColumn(UNIQUENESS_ID, monotonically_increasing_id())
      .withColumn("new", metric.fullColumn.get).orderBy("unique")
    resultDf
      .collect().map(_.getAs[Any]("new")) shouldBe Seq(true, true, true, null, null, null)
  }

  "return filtered row-level results for uniqueness with null on multiple columns" in withSparkSession { session =>

    val data = getDfWithUniqueColumns(session)

    val addressLength = Uniqueness(Seq("halfUniqueCombinedWithNonUnique", "nonUnique"), Option("unique > 2"),
      Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)))
    val state: Option[FrequenciesAndNumRows] = addressLength.computeStateFrom(data, Option("unique > 2"))
    val metric: DoubleMetric with FullColumn = addressLength.computeMetricFrom(state)

    // Adding column with UNIQUENESS_ID, since it's only added in VerificationResult.getRowLevelResults
    val resultDf = data.withColumn(UNIQUENESS_ID, monotonically_increasing_id())
      .withColumn("new", metric.fullColumn.get).orderBy("unique")
    resultDf
      .collect().map(_.getAs[Any]("new")) shouldBe Seq(null, null, true, true, true, true)
  }

  "return filtered row-level results for uniqueness true null" in withSparkSession { session =>

    val data = getDfWithUniqueColumns(session)

    // Explicitly setting RowLevelFilterTreatment for test purposes, this should be set at the VerificationRunBuilder
    val addressLength = Uniqueness(Seq("onlyUniqueWithOtherNonUnique"), Option("unique < 4"))
    val state: Option[FrequenciesAndNumRows] = addressLength.computeStateFrom(data, Option("unique < 4"))
    val metric: DoubleMetric with FullColumn = addressLength.computeMetricFrom(state)

    // Adding column with UNIQUENESS_ID, since it's only added in VerificationResult.getRowLevelResults
    val resultDf = data.withColumn(UNIQUENESS_ID, monotonically_increasing_id())
      .withColumn("new", metric.fullColumn.get).orderBy("unique")
    resultDf
      .collect().map(_.getAs[Any]("new")) shouldBe Seq(true, true, true, true, true, true)
  }

  "return filtered row-level results for uniqueness with true on multiple columns" in withSparkSession { session =>

    val data = getDfWithUniqueColumns(session)

    // Explicitly setting RowLevelFilterTreatment for test purposes, this should be set at the VerificationRunBuilder
    val addressLength = Uniqueness(Seq("halfUniqueCombinedWithNonUnique", "nonUnique"), Option("unique > 2"))
    val state: Option[FrequenciesAndNumRows] = addressLength.computeStateFrom(data, Option("unique > 2"))
    val metric: DoubleMetric with FullColumn = addressLength.computeMetricFrom(state)

    // Adding column with UNIQUENESS_ID, since it's only added in VerificationResult.getRowLevelResults
    val resultDf = data.withColumn(UNIQUENESS_ID, monotonically_increasing_id())
      .withColumn("new", metric.fullColumn.get).orderBy("unique")
    resultDf
      .collect().map(_.getAs[Any]("new")) shouldBe Seq(true, true, true, true, true, true)
  }
}
