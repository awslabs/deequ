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
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.element_at
import org.apache.spark.sql.types.DoubleType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MinimumTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {
  private val tempColName = "new"

  private def getValuesDF(df: DataFrame, outcomeColumn: Column): Seq[Row] = {
    df.withColumn(tempColName, element_at(outcomeColumn, 2).cast(DoubleType)).collect()
  }

  "Min" should {
    "return row-level results for columns" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val att1Minimum = Minimum("att1")
      val state: Option[MinState] = att1Minimum.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Minimum.computeMetricFrom(state)

      val values = getValuesDF(data, metric.fullColumn.get).map(_.getAs[Double](tempColName))
      values shouldBe Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    }

    "return row-level results for columns with null" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val att1Minimum = Minimum("attNull")
      val state: Option[MinState] = att1Minimum.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Minimum.computeMetricFrom(state)

      val values = getValuesDF(data, metric.fullColumn.get)
        .map(r => if (r == null) null else r.getAs[Double](tempColName))
      values shouldBe Seq(null, null, null, 5.0, 6.0, 7.0)
    }
  }
}
