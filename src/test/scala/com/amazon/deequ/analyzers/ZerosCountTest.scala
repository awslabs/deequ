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

class ZerosCountTest extends AnyWordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "ZerosCount" should {

    "count zeros correctly for numeric data" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      // att2 has values [0, 0, 0, 5, 6, 7] -> 3 zeros
      val result = ZerosCount("att2").calculate(df)
      result.value shouldBe Success(3.0)
    }

    "return 0 when no zeros exist" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      // att1 has values [1, 2, 3, 4, 5, 6] -> 0 zeros
      val result = ZerosCount("att1").calculate(df)
      result.value shouldBe Success(0.0)
    }

    "count all rows when all values are zero" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(0.0, 0.0, 0.0).toDF("value")
      ZerosCount("value").calculate(df).value shouldBe Success(3.0)
    }

    "fail for non-numeric data" in withSparkSession { session =>
      val df = getDfFull(session)
      assert(ZerosCount("att1").calculate(df).value.isFailure)
    }

    "compute correct value with where clause" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      // att2 [0,0,0,5,6,7], filter item != '1' -> [0,0,5,6,7] -> 2 zeros
      val result = ZerosCount("att2", where = Some("item != '1'")).calculate(df)
      result.value shouldBe Success(2.0)
    }

    "ignore null values" in withSparkSession { session =>
      val schema = StructType(Array(StructField("value", DoubleType, nullable = true)))
      val rows = session.sparkContext.parallelize(Seq(
        Row(0.0), Row(null), Row(0.0), Row(null), Row(5.0)
      ))
      val df = session.createDataFrame(rows, schema)
      // nulls are not zeros -> 2 zeros
      ZerosCount("value").calculate(df).value shouldBe Success(2.0)
    }

    "return 0 for all-null column" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        (None: Option[Double]),
        (None: Option[Double]),
        (None: Option[Double])
      ).toDF("value")
      ZerosCount("value").calculate(df).value shouldBe Success(0.0)
    }

    "work with IntegerType column" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(0, 1, 0, 2, 0).toDF("value")
      ZerosCount("value").calculate(df).value shouldBe Success(3.0)
    }

    "work with LongType column" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(0L, 1L, 0L).toDF("value")
      ZerosCount("value").calculate(df).value shouldBe Success(2.0)
    }

    "work with FloatType column" in withSparkSession { session =>
      val schema = StructType(Array(StructField("value", FloatType, nullable = false)))
      val rows = session.sparkContext.parallelize(Seq(Row(0.0f), Row(1.0f), Row(0.0f)))
      val df = session.createDataFrame(rows, schema)
      ZerosCount("value").calculate(df).value shouldBe Success(2.0)
    }

    "work with DecimalType column" in withSparkSession { session =>
      val schema = StructType(Array(StructField("value", DecimalType(10, 2), nullable = false)))
      val rows = session.sparkContext.parallelize(Seq(
        Row(new java.math.BigDecimal("0.00")),
        Row(new java.math.BigDecimal("1.00")),
        Row(new java.math.BigDecimal("0.00"))
      ))
      val df = session.createDataFrame(rows, schema)
      ZerosCount("value").calculate(df).value shouldBe Success(2.0)
    }

    "produce correct metric metadata" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val metric = ZerosCount("att1").calculate(df)
      metric shouldBe a[DoubleMetric]
      metric.entity shouldBe Entity.Column
      metric.name shouldBe "ZerosCount"
      metric.instance shouldBe "att1"
    }

    "merge states correctly" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val analyzer = ZerosCount("att2")
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
}
