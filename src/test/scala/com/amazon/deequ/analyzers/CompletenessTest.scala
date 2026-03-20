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

      // Explicitly setting RowLevelFilterTreatment for test purposes, this should be set at the VerificationRunBuilder
      val completenessAtt2 = Completeness("att2", Option("att1 = \"a\""),
                              Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)))
      val state = completenessAtt2.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = completenessAtt2.computeMetricFrom(state)

      val df = data.withColumn("new", metric.fullColumn.get)
      df.show(false)
      df.collect().map(_.getAs[Any]("new")).toSeq  shouldBe
        Seq(true, null, false, true, null, true)
    }

    "return row-level results for columns filtered as true" in withSparkSession { session =>

      val data = getDfCompleteAndInCompleteColumns(session)

      // Explicitly setting RowLevelFilterTreatment for test purposes, this should be set at the VerificationRunBuilder
      val completenessAtt2 = Completeness("att2", Option("att1 = \"a\""))
      val state = completenessAtt2.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = completenessAtt2.computeMetricFrom(state)

      val df = data.withColumn("new", metric.fullColumn.get)
      df.show(false)
      df.collect().map(_.getAs[Any]("new")).toSeq shouldBe
        Seq(true, true, false, true, true, true)
    }

    "return Failure metric with fullColumn when where clause filters all rows" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val analyzer = Completeness("att1", where = Some("att1 > 100"))
      val state = analyzer.computeStateFrom(data)
      val metric = analyzer.computeMetricFrom(state)

      state.map(s => (s.numMatches, s.count)) shouldBe Some((0L, 0L))
      metric.value.isFailure shouldBe true
      metric.fullColumn shouldBe defined
    }

    "return null row-level results when where filters all rows with NULL" in
      withSparkSession { session =>
        val data = getDfWithNumericValues(session)

        val analyzer = Completeness("att1", where = Some("att1 > 100"),
          analyzerOptions = Some(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)))
        val state = analyzer.computeStateFrom(data)
        val metric = analyzer.computeMetricFrom(state)

        metric.fullColumn shouldBe defined
        val results = data.withColumn("result", metric.fullColumn.get)
          .collect().map(r => r.isNullAt(r.fieldIndex("result")))
        results.foreach(_ shouldBe true)
      }
  }
}
