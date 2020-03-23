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
import com.amazon.deequ.analyzers.Histogram.NullFieldReplacement
import com.amazon.deequ.metrics.{BucketDistribution, BucketValue, Distribution, DistributionValue}
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
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
    assert(expected.kll == actual.kll)
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

    "return correct columnProfiles with predefined dataType" in withSparkSession { session =>

      val data = getDfCompleteAndInCompleteColumns(session)

      val actualColumnProfile = ColumnProfiler.profile(data, Option(Seq("item")), false, 1,
        predefinedTypes =
          Map[String, DataTypeInstances.Value]("item"-> DataTypeInstances.String))
        .profiles("item")

      val expectedColumnProfile = StandardColumnProfile(
        "item",
        1.0,
        6,
        DataTypeInstances.String,
        false,
        Map(),
        None)

      assert(actualColumnProfile == expectedColumnProfile)
    }

    "return correct columnProfiles without predefined dataType" in withSparkSession { session =>

      val data = getDfCompleteAndInCompleteColumns(session)

      val actualColumnProfile = ColumnProfiler.profile(data, Option(Seq("att2")), false, 1,
        predefinedTypes =
          Map[String, DataTypeInstances.Value]("item"-> DataTypeInstances.String))
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
    "return correct NumericColumnProfiles for numeric String DataType columns when " +
      "kllProfiling disabled" in withSparkSession { session =>

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

    "return correct NumericColumnProfiles for numeric String DataType columns when " +
      " kllProfiling enabled" in withSparkSession { session =>

        val data = getDfCompleteAndInCompleteColumns(session)

        val actualColumnProfile = ColumnProfiler.profile(data, Option(Seq("item")), false, 1,
          kllProfiling = true)
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
          Some(BucketDistribution(
            List(BucketValue(1.0, 1.05, 1), BucketValue(1.05, 1.1, 0), BucketValue(1.1, 1.15, 0),
              BucketValue (1.15, 1.2, 0), BucketValue(1.2, 1.25, 0), BucketValue(1.25, 1.3, 0),
              BucketValue(1.3, 1.35, 0), BucketValue(1.35, 1.4, 0), BucketValue(1.4, 1.45, 0),
              BucketValue(1.45, 1.5, 0), BucketValue(1.5, 1.55, 0), BucketValue(1.55, 1.6, 0),
              BucketValue(1.6, 1.65, 0), BucketValue(1.65, 1.7, 0), BucketValue(1.7, 1.75, 0),
              BucketValue(1.75, 1.8, 0), BucketValue(1.8, 1.85, 0), BucketValue(1.85, 1.9, 0),
              BucketValue(1.9, 1.95, 0), BucketValue(1.95, 2.0, 0), BucketValue(2.0, 2.05, 1),
              BucketValue(2.05, 2.1, 0), BucketValue(2.1, 2.15, 0), BucketValue(2.15, 2.2, 0),
              BucketValue(2.2, 2.25, 0), BucketValue(2.25, 2.3, 0), BucketValue(2.3, 2.35, 0),
              BucketValue(2.35, 2.4, 0), BucketValue(2.4, 2.45, 0), BucketValue(2.45, 2.5, 0),
              BucketValue(2.5, 2.55, 0), BucketValue(2.55, 2.6, 0), BucketValue(2.6, 2.65, 0),
              BucketValue(2.65, 2.7, 0), BucketValue(2.7, 2.75, 0), BucketValue(2.75, 2.8, 0),
              BucketValue(2.8, 2.85, 0), BucketValue(2.85, 2.9, 0), BucketValue(2.9, 2.95, 0),
              BucketValue(2.95, 3.0, 0), BucketValue(3.0, 3.05, 1), BucketValue(3.05, 3.1, 0),
              BucketValue(3.1, 3.15, 0), BucketValue(3.15, 3.2, 0), BucketValue(3.2, 3.25, 0),
              BucketValue(3.25, 3.3, 0), BucketValue(3.3, 3.35, 0), BucketValue(3.35, 3.4, 0),
              BucketValue(3.4, 3.45, 0), BucketValue(3.45, 3.5, 0), BucketValue(3.5, 3.55, 0),
              BucketValue(3.55, 3.6, 0), BucketValue(3.6, 3.65, 0), BucketValue(3.65, 3.7, 0),
              BucketValue(3.7, 3.75, 0), BucketValue(3.75, 3.8, 0), BucketValue(3.8, 3.85, 0),
              BucketValue(3.85, 3.9, 0), BucketValue(3.9, 3.95, 0), BucketValue(3.95, 4.0, 0),
              BucketValue(4.0, 4.05, 1), BucketValue(4.05, 4.1, 0), BucketValue(4.1, 4.15, 0),
              BucketValue(4.15, 4.2, 0), BucketValue(4.2, 4.25, 0), BucketValue(4.25, 4.3, 0),
              BucketValue(4.3, 4.35, 0), BucketValue(4.35, 4.4, 0), BucketValue(4.4, 4.45, 0),
              BucketValue(4.45, 4.5, 0), BucketValue(4.5, 4.55, 0), BucketValue(4.55, 4.6, 0),
              BucketValue(4.6, 4.65, 0), BucketValue(4.65, 4.7, 0), BucketValue(4.7, 4.75, 0),
              BucketValue(4.75, 4.8, 0), BucketValue(4.8, 4.85, 0), BucketValue(4.85, 4.9, 0),
              BucketValue(4.9, 4.95, 0), BucketValue(4.95, 5.0, 0), BucketValue(5.0, 5.05, 1),
              BucketValue(5.05, 5.1, 0), BucketValue(5.1, 5.15, 0), BucketValue(5.15, 5.2, 0),
              BucketValue(5.2, 5.25, 0), BucketValue(5.25, 5.3, 0), BucketValue(5.3, 5.35, 0),
              BucketValue(5.35, 5.4, 0), BucketValue(5.4, 5.45, 0), BucketValue(5.45, 5.5, 0),
              BucketValue(5.5, 5.55, 0), BucketValue(5.55, 5.6, 0), BucketValue(5.6, 5.65, 0),
              BucketValue(5.65, 5.7, 0), BucketValue(5.7, 5.75, 0), BucketValue(5.75, 5.8, 0),
              BucketValue(5.8, 5.85, 0), BucketValue(5.85, 5.9, 0), BucketValue(5.9, 5.95, 0),
              BucketValue(5.95, 6.0, 1)),
            List(0.64, 2048.0),
            Array(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)))),
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

      val actualColumnProfile = ColumnProfiler.profile(data, Option(Seq("att1")), false, 1)
        .profiles("att1")

      val expectedColumnProfile = NumericColumnProfile(
        "att1",
        1.0,
        6,
        DataTypeInstances.Fractional,
        false,
        Map.empty,
        None,
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

    "return correct Histograms for string columns" in withSparkSession { session =>

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
          NullFieldReplacement -> DistributionValue(2, 0.3333333333333333)), 3)))

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
      assert(histogram(NullFieldReplacement).absolute == 1)
      assert(histogram(NullFieldReplacement).ratio == 1.0 / nRows)
    }

    "return histograms for IntegerType columns" in withSparkSession { session =>
      val attribute = "attribute"
      val nRows = 6
      val data = com.amazon.deequ.dataFrameWithColumn(
        attribute,
        IntegerType,
        session,
        Row(2147483647),
        Row(2147483647),
        Row(2147483647),
        Row(2),
        Row(2),
        Row(null)
      )

      val actualColumnProfile = ColumnProfiler.profile(data).profiles(attribute)

      assert(actualColumnProfile.histogram.isDefined)

      val histogram = actualColumnProfile.histogram.get

      assert(histogram("2147483647").absolute == 3L)
      assert(histogram("2147483647").ratio == 3.0 / nRows)
      assert(histogram("2").absolute == 2L)
      assert(histogram("2").ratio == 2.0 / nRows)
      assert(histogram(NullFieldReplacement).absolute == 1)
      assert(histogram(NullFieldReplacement).ratio == 1.0 / nRows)
    }

    "return histograms for LongType columns" in withSparkSession { session =>
      val attribute = "attribute"
      val nRows = 6
      val data = com.amazon.deequ.dataFrameWithColumn(
        attribute,
        LongType,
        session,
        Row(1L),
        Row(1L),
        Row(1L),
        Row(2L),
        Row(2L),
        Row(null)
      )

      val actualColumnProfile = ColumnProfiler.profile(data).profiles(attribute)

      assert(actualColumnProfile.histogram.isDefined)

      val histogram = actualColumnProfile.histogram.get

      assert(histogram("1").absolute == 3L)
      assert(histogram("1").ratio == 3.0 / nRows)
      assert(histogram("2").absolute == 2L)
      assert(histogram("2").ratio == 2.0 / nRows)
      assert(histogram(NullFieldReplacement).absolute == 1)
      assert(histogram(NullFieldReplacement).ratio == 1.0 / nRows)
    }

    "return histograms for DoubleType columns" in withSparkSession { session =>
      val attribute = "attribute"
      val nRows = 6
      val data = com.amazon.deequ.dataFrameWithColumn(
        attribute,
        DoubleType,
        session,
        Row(1.0),
        Row(1.0),
        Row(1.0),
        Row(2.0),
        Row(2.0),
        Row(null)
      )

      val actualColumnProfile = ColumnProfiler.profile(data).profiles(attribute)

      assert(actualColumnProfile.histogram.isDefined)

      val histogram = actualColumnProfile.histogram.get

      assert(histogram("1.0").absolute == 3L)
      assert(histogram("1.0").ratio == 3.0 / nRows)
      assert(histogram("2.0").absolute == 2L)
      assert(histogram("2.0").ratio == 2.0 / nRows)
      assert(histogram(NullFieldReplacement).absolute == 1)
      assert(histogram(NullFieldReplacement).ratio == 1.0 / nRows)
    }

    "return histograms for FloatType columns" in withSparkSession { session =>
      val attribute = "attribute"
      val nRows = 6
      val data = com.amazon.deequ.dataFrameWithColumn(
        attribute,
        FloatType,
        session,
        Row(1.0f),
        Row(1.0f),
        Row(1.0f),
        Row(2.0f),
        Row(2.0f),
        Row(null)
      )

      val actualColumnProfile = ColumnProfiler.profile(data).profiles(attribute)

      assert(actualColumnProfile.histogram.isDefined)

      val histogram = actualColumnProfile.histogram.get

      assert(histogram("1.0").absolute == 3L)
      assert(histogram("1.0").ratio == 3.0 / nRows)
      assert(histogram("2.0").absolute == 2L)
      assert(histogram("2.0").ratio == 2.0 / nRows)
      assert(histogram(NullFieldReplacement).absolute == 1)
      assert(histogram(NullFieldReplacement).ratio == 1.0 / nRows)
    }

    "return histograms for ShortType columns" in withSparkSession { session =>
      val attribute = "attribute"
      val nRows = 6
      val data = com.amazon.deequ.dataFrameWithColumn(
        attribute,
        ShortType,
        session,
        Row(1: Short),
        Row(1: Short),
        Row(1: Short),
        Row(2: Short),
        Row(2: Short),
        Row(null)
      )

      val actualColumnProfile = ColumnProfiler.profile(data).profiles(attribute)

      assert(actualColumnProfile.histogram.isDefined)

      val histogram = actualColumnProfile.histogram.get

      assert(histogram("1").absolute == 3L)
      assert(histogram("1").ratio == 3.0 / nRows)
      assert(histogram("2").absolute == 2L)
      assert(histogram("2").ratio == 2.0 / nRows)
      assert(histogram(NullFieldReplacement).absolute == 1)
      assert(histogram(NullFieldReplacement).ratio == 1.0 / nRows)
    }
  }

  "return correct profile for the Titanic dataset" in withSparkSession { session =>
    val data = session.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("test-data/titanic.csv")

    val columnProfiles = ColumnProfiler.profile(data)

    val expectedProfiles = List(
      StandardColumnProfile(
        "PassengerId",
        1.0,
        891,
        DataTypeInstances.Integral,
        false,
        Map.empty,
        None),
      StandardColumnProfile(
        "Survived",
        1.0,
        2,
        DataTypeInstances.Integral,
        false,
        Map.empty,
        None),
      StandardColumnProfile("Pclass", 1.0, 3, DataTypeInstances.Integral, false, Map.empty, None),
      StandardColumnProfile("Name", 1.0, 0, DataTypeInstances.String, true, Map.empty, None),
      StandardColumnProfile("Sex", 1.0, 2, DataTypeInstances.String, true, Map.empty, None),
      StandardColumnProfile("Ticket", 1.0, 681, DataTypeInstances.String, true, Map.empty, None),
      StandardColumnProfile("Fare", 1.0, 0, DataTypeInstances.Fractional, false, Map.empty, None),
      StandardColumnProfile("Cabin", 0.22, 0, DataTypeInstances.String, true, Map.empty, None)
    )

    assertSameColumnProfiles(columnProfiles.profiles, expectedProfiles)

  }

  private[this] def assertSameColumnProfiles(
      actualProfiles: Map[String, ColumnProfile],
      expectedProfiles: List[ColumnProfile])
    : Unit = {

    expectedProfiles.foreach { expected =>
      val actual = actualProfiles(expected.column)
      val msg = s"""(Column "${expected.column}"")"""
      assert(actual.dataType == expected.dataType, msg)
      assert(actual.completeness >= expected.completeness, msg)
      assert(actual.isDataTypeInferred == expected.isDataTypeInferred, msg)
      if (expected.approximateNumDistinctValues > 0) {
        val upperBound = 1.1 * expected.approximateNumDistinctValues
        val lowerBound = 0.9 * expected.approximateNumDistinctValues
        assert(
          actual.approximateNumDistinctValues <= upperBound &&
          actual.approximateNumDistinctValues >= lowerBound,
          msg)
      }
    }
  }
}
