/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ
package analyzers

import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Success

class VarianceTest extends AnyWordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "Variance" should {

    "compute correct value for numeric data" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val result = Variance("att1").calculate(df)
      result.value shouldBe Success(2.9166666666666665)
    }

    "equal stddev squared" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val stddev = StandardDeviation("att1").calculate(df).value.get
      val variance = Variance("att1").calculate(df).value.get
      variance shouldBe (stddev * stddev +- 1e-10)
    }

    "fail for non-numeric data" in withSparkSession { session =>
      val df = getDfFull(session)
      assert(Variance("att1").calculate(df).value.isFailure)
    }

    "compute correct value with where clause" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val result = Variance("att1", where = Some("item != '6'")).calculate(df)
      assert(result.value.isSuccess)
    }

    "produce None state from all-null column" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        (None: Option[Double]),
        (None: Option[Double]),
        (None: Option[Double])
      ).toDF("value")
      Variance("value").computeStateFrom(df) shouldBe None
    }

    "return 0 for single row" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(42.0).toDF("value")
      Variance("value").calculate(df).value shouldBe Success(0.0)
    }

    "return 0 for all identical values" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(5.0, 5.0, 5.0, 5.0).toDF("value")
      Variance("value").calculate(df).value shouldBe Success(0.0)
    }

    "compute on non-null values when column has mixed nulls" in withSparkSession { session =>
      val schema = StructType(Array(StructField("value", DoubleType, nullable = true)))
      val rows = session.sparkContext.parallelize(Seq(
        Row(1.0), Row(null), Row(3.0), Row(null), Row(5.0)
      ))
      val df = session.createDataFrame(rows, schema)
      // non-null values: 1, 3, 5 -> mean=3, variance = ((1-3)^2 + (3-3)^2 + (5-3)^2) / 3 = 8/3
      val result = Variance("value").calculate(df)
      result.value.get shouldBe (8.0 / 3.0 +- 1e-10)
    }

    "work with DoubleType column" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(1.0, 2.0, 3.0).toDF("value")
      val result = Variance("value").calculate(df)
      // variance of [1,2,3] = 2/3
      result.value.get shouldBe (2.0 / 3.0 +- 1e-10)
    }

    "work with LongType column" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(1L, 2L, 3L).toDF("value")
      val result = Variance("value").calculate(df)
      result.value.get shouldBe (2.0 / 3.0 +- 1e-10)
    }

    "work with FloatType column" in withSparkSession { session =>
      val schema = StructType(Array(StructField("value", FloatType, nullable = false)))
      val rows = session.sparkContext.parallelize(Seq(Row(1.0f), Row(2.0f), Row(3.0f)))
      val df = session.createDataFrame(rows, schema)
      val result = Variance("value").calculate(df)
      result.value.get shouldBe (2.0 / 3.0 +- 1e-6)
    }

    "work with DecimalType column" in withSparkSession { session =>
      val schema = StructType(Array(StructField("value", DecimalType(10, 2), nullable = false)))
      val rows = session.sparkContext.parallelize(Seq(
        Row(new java.math.BigDecimal("1.00")),
        Row(new java.math.BigDecimal("2.00")),
        Row(new java.math.BigDecimal("3.00"))
      ))
      val df = session.createDataFrame(rows, schema)
      val result = Variance("value").calculate(df)
      result.value.get shouldBe (2.0 / 3.0 +- 1e-10)
    }

    "produce correct metric metadata" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val metric = Variance("att1").calculate(df)
      metric shouldBe a[DoubleMetric]
      metric.entity shouldBe Entity.Column
      metric.name shouldBe "Variance"
      metric.instance shouldBe "att1"
    }

    "merge states correctly" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val analyzer = Variance("att1")
      val overall = analyzer.calculate(df)

      val half1 = session.createDataFrame(
        session.sparkContext.parallelize(Seq(
          Row("1", 1, 0, 0, null), Row("2", 2, 0, 0, null), Row("3", 3, 0, 0, null))),
        df.schema)
      val half2 = session.createDataFrame(
        session.sparkContext.parallelize(Seq(
          Row("4", 4, 5, 4, 5), Row("5", 5, 6, 6, 6), Row("6", 6, 7, 7, 7))),
        df.schema)

      val state1 = analyzer.computeStateFrom(half1)
      val state2 = analyzer.computeStateFrom(half2)
      val merged = Analyzers.merge(state1, state2)
      val mergedMetric = analyzer.computeMetricFrom(merged)

      mergedMetric.value shouldBe overall.value
    }
  }

  "VarianceState" should {
    "throw on construction with n = 0" in {
      an[IllegalArgumentException] should be thrownBy {
        VarianceState(0.0, 0.0, 0.0)
      }
    }
  }
}
