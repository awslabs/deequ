package com.amazon.deequ.profiles

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers.DataTypeInstances
import com.amazon.deequ.metrics.{Distribution, DistributionValue}
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

class ColumnProfilerTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

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

      assert(actualColumnProfile == expectedColumnProfile)
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

      assert(actualColumnProfile == expectedColumnProfile)
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
        true,
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
  }

}
