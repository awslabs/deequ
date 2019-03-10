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
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.runtime.spark.executor.EmptyStateException
import com.amazon.deequ.runtime.spark.operators._
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class NullHandlingTests extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  private[this] def dataWithNullColumns(session: SparkSession): DataFrame = {

    val schema = StructType(Array(
      StructField("stringCol", StringType, nullable = true),
      StructField("numericCol", DoubleType, nullable = true),
      StructField("numericCol2", DoubleType, nullable = true),
        StructField("numericCol3", DoubleType, nullable = true)
    ))

    val rows = session.sparkContext.parallelize(Seq(
      Row(null, null, null, 1.0),
      Row(null, null, null, 2.0),
      Row(null, null, null, 3.0),
      Row(null, null, null, 4.0),
      Row(null, null, null, 5.0),
      Row(null, null, null, 6.0),
      Row(null, null, null, 7.0),
      Row(null, null, null, 8.0)),
      numSlices = 2)

    session.createDataFrame(rows, schema)
  }

  "Null columns" should {

    "produce correct states" in withSparkSession { session =>

      val data = dataWithNullColumns(session)

      SizeOp().computeStateFrom(data) shouldBe Some(NumMatches(8))
      CompletenessOp("stringCol").computeStateFrom(data) shouldBe Some(NumMatchesAndCount(0, 8))

      MeanOp("numericCol").computeStateFrom(data) shouldBe None
      StandardDeviationOp("numericCol").computeStateFrom(data) shouldBe None
      MinimumOp("numericCol").computeStateFrom(data) shouldBe None
      MaximumOp("numericCol").computeStateFrom(data) shouldBe None

      DataTypeOp("stringCol").computeStateFrom(data) shouldBe
        Some(DataTypeHistogram(8L, 0L, 0L, 0L, 0L))

      SumOp("numericCol").computeStateFrom(data) shouldBe None
      ApproxQuantileOp("numericCol", 0.5).computeStateFrom(data) shouldBe None

      val stringColFrequenciesAndNumRows = CountDistinctOp("stringCol").computeStateFrom(data)
      assert(stringColFrequenciesAndNumRows.isDefined)

      stringColFrequenciesAndNumRows.get.numRows shouldBe 8
      stringColFrequenciesAndNumRows.get.frequencies.count() shouldBe 0L

      val numericColFrequenciesAndNumRows = MutualInformationOp("numericCol", "numericCol2")
        .computeStateFrom(data)

      assert(numericColFrequenciesAndNumRows.isDefined)

      numericColFrequenciesAndNumRows.get.numRows shouldBe 8
      numericColFrequenciesAndNumRows.get.frequencies.count() shouldBe 0L


      CorrelationOp("numericCol", "numericCol2").computeStateFrom(data) shouldBe None
    }

    "produce correct metrics" in withSparkSession { session =>

      val data = dataWithNullColumns(session)

      SizeOp().calculate(data).value shouldBe Success(8.0)
      CompletenessOp("stringCol").calculate(data).value shouldBe Success(0.0)

      assertFailedWithEmptyState(MeanOp("numericCol").calculate(data))

      assertFailedWithEmptyState(StandardDeviationOp("numericCol").calculate(data))
      assertFailedWithEmptyState(MinimumOp("numericCol").calculate(data))
      assertFailedWithEmptyState(MaximumOp("numericCol").calculate(data))

      val dataTypeDistribution = DataTypeOp("stringCol").calculate(data).value.get
      dataTypeDistribution.values("Unknown").ratio shouldBe 1.0

      assertFailedWithEmptyState(SumOp("numericCol").calculate(data))
      assertFailedWithEmptyState(ApproxQuantileOp("numericCol", 0.5).calculate(data))

      CountDistinctOp("stringCol").calculate(data).value shouldBe Success(0.0)
      ApproxCountDistinctOp("stringCol").calculate(data).value shouldBe Success(0.0)

      assertFailedWithEmptyState(EntropyOp("stringCol").calculate(data))
      assertFailedWithEmptyState(MutualInformationOp("numericCol", "numericCol2").calculate(data))
      assertFailedWithEmptyState(MutualInformationOp("numericCol", "numericCol3").calculate(data))
      assertFailedWithEmptyState(CorrelationOp("numericCol", "numericCol2").calculate(data))
      assertFailedWithEmptyState(CorrelationOp("numericCol", "numericCol3").calculate(data))
    }

//    "include analyzer name in EmptyStateExceptions" in withSparkSession { session =>
//
//      val data = dataWithNullColumns(session)
//
//      val metricResult = MeanOp("numericCol").calculate(data).value
//
//      assert(metricResult.isFailure)
//
//      val exceptionMessage = metricResult.failed.get.getMessage
//
//      assert(exceptionMessage == "Empty state for analyzer Mean(numericCol,None), " +
//        "all input values were NULL.")
//
//    }
  }

  private[this] def assertFailedWithEmptyState(metric: DoubleMetric): Unit = {
    assert(metric.value.isFailure)
    assert(metric.value.failed.get.isInstanceOf[EmptyStateException])
  }


}
