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

class KurtosisTest extends AnyWordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "Kurtosis" should {

    "return negative excess kurtosis for uniform-like data" in
      withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      // [1,2,3,4,5,6] is platykurtic (flatter than normal)
      val result = Kurtosis("att1").calculate(df).value.get
      result should be < 0.0
      result shouldBe (-1.2685714285714285 +- 1e-10)
    }

    "return 0 for all identical values" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(5.0, 5.0, 5.0, 5.0).toDF("value")
      Kurtosis("value").calculate(df).value shouldBe Success(0.0)
    }

    "fail for non-numeric data" in withSparkSession { session =>
      val df = getDfFull(session)
      assert(Kurtosis("att1").calculate(df).value.isFailure)
    }

    "compute correct value with where clause" in
      withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val result = Kurtosis("att1", where = Some("item != '6'"))
        .calculate(df)
      assert(result.value.isSuccess)
    }

    "produce None state from all-null column" in
      withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        (None: Option[Double]),
        (None: Option[Double]),
        (None: Option[Double])
      ).toDF("value")
      Kurtosis("value").computeStateFrom(df) shouldBe None
    }

    "work with LongType column" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(1L, 2L, 3L, 4L, 5L, 6L).toDF("value")
      val result = Kurtosis("value").calculate(df).value.get
      result should be < 0.0
    }

    "produce correct metric metadata" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val metric = Kurtosis("att1").calculate(df)
      metric shouldBe a[DoubleMetric]
      metric.entity shouldBe Entity.Column
      metric.name shouldBe "Kurtosis"
      metric.instance shouldBe "att1"
    }

    "merge states correctly" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val analyzer = Kurtosis("att1")
      val overall = analyzer.calculate(df)

      val half1 = session.createDataFrame(
        session.sparkContext.parallelize(Seq(
          Row("1", 1, 0, 0, null),
          Row("2", 2, 0, 0, null),
          Row("3", 3, 0, 0, null))),
        df.schema)
      val half2 = session.createDataFrame(
        session.sparkContext.parallelize(Seq(
          Row("4", 4, 5, 4, 5),
          Row("5", 5, 6, 6, 6),
          Row("6", 6, 7, 7, 7))),
        df.schema)

      val state1 = analyzer.computeStateFrom(half1)
      val state2 = analyzer.computeStateFrom(half2)
      val merged = Analyzers.merge(state1, state2)
      val mergedMetric = analyzer.computeMetricFrom(merged)

      mergedMetric.value.get shouldBe
        (overall.value.get +- 1e-6)
    }
  }

  "KurtosisState" should {
    "throw on construction with n = 0" in {
      an[IllegalArgumentException] should be thrownBy {
        KurtosisState(0.0, 0.0, 0.0, 0.0, 0.0)
      }
    }
  }
}
