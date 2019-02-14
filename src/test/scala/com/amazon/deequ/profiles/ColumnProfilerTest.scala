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

package com.amazon.deequ.profiles

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers.DataTypeInstances
import com.amazon.deequ.metrics.{Distribution, DistributionValue}
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BooleanType
import org.scalatest.{Matchers, WordSpec}

class ColumnProfilerTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  def assertProfilesEqual(expected: NumericColumnProfile, actual: NumericColumnProfile): Unit = {

    assert(expected.column == actual.column)
    assert(expected.completeness == actual.completeness)
    assert(math.abs(expected.approximateNumDistinctValues -
      actual.approximateNumDistinctValues) <= 1)
    assert(expected.dataType == actual.dataType)
    assert(expected.isDataTypeInferred == expected.isDataTypeInferred)
    assert(expected.typeCounts == actual.typeCounts)
    assert(expected.histogram == actual.histogram)
    assert(expected.mean == actual.mean)
    assert(expected.maximum == actual.maximum)
    assert(expected.minimum == actual.minimum)
    assert(expected.sum == actual.sum)
    assert(expected.stdDev == actual.stdDev)
    // TODO disabled for now, as we get different results for Spark 2.2 and Spark 2.3
    // assert(expected.approxPercentiles == actual.approxPercentiles)
  }

  "Column Profiler" should {

    "return correct StandardColumnProfiles" in withSparkSession { session =>

      val data = getDfCompleteAndInCompleteColumns(session)

      val actualColumnProfile = ColumnProfiler.profile(data, Option(Seq("att2")), false, 1)
        .profiles("att2")

      val expectedColumnProfile = StandardColumnProfile(
        "att2",
        2.0 / 3.0,
        2,
        DataTypeInstances.String,
        true,
        Map(
          "Boolean" -> 0,
          "Fractional" -> 0,
          "Integral" -> 0,
          "Unknown" -> 2,
          "String" -> 4
        ),
        None)

      assert(actualColumnProfile == expectedColumnProfile)
    }

    "return correct NumericColumnProfiles for numeric String DataType columns" in
      withSparkSession { session =>

      val data = getDfCompleteAndInCompleteColumns(session)

      val actualColumnProfile = ColumnProfiler.profile(data, Option(Seq("item")), false, 1)
        .profiles("item")

      val expectedColumnProfile = NumericColumnProfile(
        "item",
        1.0,
        6,
        DataTypeInstances.Integral,
        true,
        Map(
          "Boolean" -> 0,
          "Fractional" -> 0,
          "Integral" -> 6,
          "Unknown" -> 0,
          "String" -> 0
        ),
        None,
        Some(3.5),
        Some(6.0),
        Some(1.0),
        Some(21.0),
        Some(1.707825127659933),
        Some(Seq(1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0,
          2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0,
          2.0, 2.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0,
          4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
          5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 6.0, 6.0, 6.0, 6.0, 6.0,
          6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0)))

        assertProfilesEqual(expectedColumnProfile,
          actualColumnProfile.asInstanceOf[NumericColumnProfile])
    }

    "return correct NumericColumnProfiles for numeric columns with correct DataType" in
      withSparkSession { session =>

      val data = getDfWithNumericFractionalValues(session)

      val actualColumnProfile = ColumnProfiler.profile(data, Option(Seq("]att1[")), false, 1)
        .profiles("]att1[")

      val expectedColumnProfile = NumericColumnProfile(
        "]att1[",
        1.0,
        6,
        DataTypeInstances.Fractional,
        false,
        Map.empty,
        None,
        Some(3.5),
        Some(6.0),
        Some(1.0),
        Some(21.0),
        Some(1.707825127659933),
        Some(Seq(1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0,
          2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0,
          2.0, 2.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0,
          4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
          5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 6.0, 6.0, 6.0, 6.0, 6.0,
          6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0)))

        assertProfilesEqual(expectedColumnProfile,
          actualColumnProfile.asInstanceOf[NumericColumnProfile])
    }

    "return correct Histograms" in withSparkSession { session =>

      val data = getDfCompleteAndInCompleteColumns(session)

      val actualColumnProfile = ColumnProfiler.profile(data, Option(Seq("att2")), false, 10)
        .profiles("att2")

      val expectedColumnProfile = StandardColumnProfile(
        "att2",
        2.0 / 3.0,
        2,
        DataTypeInstances.String,
        isDataTypeInferred = true,
        Map(
          "Boolean" -> 0,
          "Fractional" -> 0,
          "Integral" -> 0,
          "Unknown" -> 2,
          "String" -> 4
        ),
        Some(Distribution(Map(
          "d" -> DistributionValue(1, 0.16666666666666666),
          "f" -> DistributionValue(3, 0.5),
          "NullValue" -> DistributionValue(2, 0.3333333333333333)), 3)))

      assert(actualColumnProfile == expectedColumnProfile)
    }

    "return histograms for boolean columns" in withSparkSession { session =>
      val attribute = "attribute"
      val nRows = 6
      val data = com.amazon.deequ.dataFrameWithColumn(
        attribute,
        BooleanType,
        session,
        Row(true),
        Row(true),
        Row(true),
        Row(false),
        Row(false),
        Row(null)
      )

      val actualColumnProfile = ColumnProfiler.profile(data).profiles(attribute)

      assert(actualColumnProfile.histogram.isDefined)

      val histogram = actualColumnProfile.histogram.get

      assert(histogram("true").absolute == 3L)
      assert(histogram("true").ratio == 3.0 / nRows)
      assert(histogram("false").absolute == 2L)
      assert(histogram("false").ratio == 2.0 / nRows)
      assert(histogram("NullValue").absolute == 1)
      assert(histogram("NullValue").ratio == 1.0 / nRows)
    }
  }

}
