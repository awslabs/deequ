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

class MaximumTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Max" should {
    "return row-level results for columns" in withSparkSession { session =>

      val data = getDfWithNumericValues(session)

      val att1Maximum = Maximum("att1")
      val state: Option[MaxState] = att1Maximum.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Maximum.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(_.getAs[Double]("new")) shouldBe
        Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    }

    "return row-level results for columns with null" in withSparkSession { session =>

      val data = getDfWithNumericValues(session)

      val att1Maximum = Maximum("attNull")
      val state: Option[MaxState] = att1Maximum.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Maximum.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Double]("new")) shouldBe
        Seq(null, null, null, 5.0, 6.0, 7.0)
    }

    "return row-level results for columns with where clause filtered as true" in withSparkSession { session =>

      val data = getDfWithNumericValues(session)

      val att1Maximum = Maximum("att1", Option("item < 4"))
      val state: Option[MaxState] = att1Maximum.computeStateFrom(data, Option("item < 4"))
      val metric: DoubleMetric with FullColumn = att1Maximum.computeMetricFrom(state)

      val result = data.withColumn("new", metric.fullColumn.get)
      result.show(false)
      result.collect().map(r =>
        if (r == null) null else r.getAs[Double]("new")) shouldBe
        Seq(1.0, 2.0, 3.0, Double.MinValue, Double.MinValue, Double.MinValue)
    }

    "return row-level results for columns with where clause filtered as null" in withSparkSession { session =>

      val data = getDfWithNumericValues(session)

      val att1Maximum = Maximum("att1", Option("item < 4"),
        Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)))
      val state: Option[MaxState] = att1Maximum.computeStateFrom(data, Option("item < 4"))
      val metric: DoubleMetric with FullColumn = att1Maximum.computeMetricFrom(state)

      val result = data.withColumn("new", metric.fullColumn.get)
      result.show(false)
      result.collect().map(r =>
        if (r == null) null else r.getAs[Double]("new")) shouldBe
        Seq(1.0, 2.0, 3.0, null, null, null)
    }
  }
}
