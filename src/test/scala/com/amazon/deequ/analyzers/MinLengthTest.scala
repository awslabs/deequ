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

class MinLengthTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "MinLength" should {
    "return row-level results for non-null columns" in withSparkSession { session =>

      val data = getDfWithStringColumns(session)

      val countryLength = MinLength("Country") // It's "India" in every row
      val state: Option[MinState] = countryLength.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = countryLength.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get)
        .collect().map(_.getAs[Double]("new")) shouldBe Seq(5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0)
    }

    "return row-level results for null columns" in withSparkSession { session =>

      val data = getEmptyColumnDataDf(session)

      val addressLength = MinLength("att3") // It's null in two rows
      val state: Option[MinState] = addressLength.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = addressLength.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get)
        .collect().map(_.getAs[Double]("new")) shouldBe Seq(1.0, 1.0, 0.0, 1.0, 0.0, 1.0)
    }

    "return row-level results for null columns with convertNull option" in withSparkSession { session =>

      val data = getEmptyColumnDataDf(session)

      // It's null in two rows
      val addressLength = MinLength("att3", analyzerOptions = Option(AnalyzerOptions(NullBehavior.Fail)))
      val state: Option[MinState] = addressLength.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = addressLength.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get)
        .collect().map(_.getAs[Double]("new")) shouldBe Seq(1.0, 1.0, Double.MinValue, 1.0, Double.MinValue, 1.0)
    }

    "return row-level results for blank strings" in withSparkSession { session =>

      val data = getEmptyColumnDataDf(session)

      val addressLength = MinLength("att1") // It's empty strings
      val state: Option[MinState] = addressLength.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = addressLength.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get)
        .collect().map(_.getAs[Double]("new")) shouldBe Seq(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    }
  }

}
