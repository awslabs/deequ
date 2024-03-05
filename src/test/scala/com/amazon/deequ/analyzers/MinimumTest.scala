/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.FullColumn
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MinimumTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {
  "Min" should {
    "return row-level results for columns" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val att1Minimum = Minimum("att1")
      val state: Option[MinState] = att1Minimum.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Minimum.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(_.getAs[Double]("new")) shouldBe
        Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    }

    "return row-level results for columns with null" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val att1Minimum = Minimum("attNull")
      val state: Option[MinState] = att1Minimum.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Minimum.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Double]("new")) shouldBe
        Seq(null, null, null, 5.0, 6.0, 7.0)
    }

    "return row-level results for columns with filtered rows" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)
      val col = "att1"
      val whereClause = "item < 4"
      val tempColName = "new"

      val analyzerOptionsFilteredRowsNull = AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)
      val analyzerOptionsFilteredRowsTrue = AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE)

      val att1MinimumFilteredRowsNull = Minimum(col, Option(whereClause), Some(analyzerOptionsFilteredRowsNull))
      val att1MinimumFilteredRowsTrue = Minimum(col, Option(whereClause), Some(analyzerOptionsFilteredRowsTrue))

      val filteredRowNullState = att1MinimumFilteredRowsNull.computeStateFrom(data, Option(whereClause))
      val filteredRowTrueState = att1MinimumFilteredRowsTrue.computeStateFrom(data, Option(whereClause))

      val filteredRowNullMetric: DoubleMetric with FullColumn =
        att1MinimumFilteredRowsNull.computeMetricFrom(filteredRowNullState)
      val filteredRowTrueMetric: DoubleMetric with FullColumn =
        att1MinimumFilteredRowsTrue.computeMetricFrom(filteredRowTrueState)

      val filteredRowNullResult = data.withColumn(tempColName, filteredRowNullMetric.fullColumn.get)
      val filteredRowTrueResult = data.withColumn(tempColName, filteredRowTrueMetric.fullColumn.get)

      Seq(filteredRowNullResult, filteredRowTrueResult).foreach { result =>
        result.collect().map(r =>
          if (r == null) null else r.getAs[Double](tempColName)) shouldBe
          Seq(1.0, 2.0, 3.0, null, null, null)
      }
    }
  }
}
