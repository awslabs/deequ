/**
 * Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.deequ.utilities.FilteredRow
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Success

class CompletenessTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Completeness" should {
    "return row-level results for columns" in withSparkSession { session =>

      val data = getDfWithStringColumns(session)

      val completenessCountry = Completeness("Address Line 3")
      val state = completenessCountry.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = completenessCountry.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(_.getAs[Boolean]("new")) shouldBe
        Seq(true, true, true, true, false, true, true, false)
    }

    "return row-level results for columns filtered as null" in withSparkSession { session =>

      val data = getDfCompleteAndInCompleteColumns(session)

      // Explicitly setting RowLevelFilterTreatment for test purposes, but this should be set at the VerificationRunBuilder
      val completenessAtt2 = Completeness("att2", Option("att1 = \"a\"")).withRowLevelFilterTreatment(FilteredRow.NULL)
      val state = completenessAtt2.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = completenessAtt2.computeMetricFrom(state)

      val df = data.withColumn("new", metric.fullColumn.get)
      df.show(false)
      df.collect().map(_.getAs[Any]("new")).toSeq  shouldBe
        Seq(true, null, false, true, null, true)
    }

    "return row-level results for columns filtered as true" in withSparkSession { session =>

      val data = getDfCompleteAndInCompleteColumns(session)

      // Explicitly setting RowLevelFilterTreatment for test purposes, but this should be set at the VerificationRunBuilder
      val completenessAtt2 = Completeness("att2", Option("att1 = \"a\"")).withRowLevelFilterTreatment(FilteredRow.TRUE)
      val state = completenessAtt2.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = completenessAtt2.computeMetricFrom(state)

      val df = data.withColumn("new", metric.fullColumn.get)
      df.show(false)
      df.collect().map(_.getAs[Any]("new")).toSeq shouldBe
        Seq(true, true, false, true, true, true)
    }
  }
}
