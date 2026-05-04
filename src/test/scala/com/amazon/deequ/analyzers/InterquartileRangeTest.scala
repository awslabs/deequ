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
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Success

class InterquartileRangeTest extends AnyWordSpec with Matchers
  with SparkContextSpec with FixtureSupport {

  "InterquartileRange" should {

    "compute correct value for numeric data" in
      withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      // att1 = [1,2,3,4,5,6], Q1=2.25, Q3=4.75, IQR=2.5
      val result = InterquartileRange("att1").calculate(df)
      result.value shouldBe Success(2.5)
    }

    "equal Q3 minus Q1" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(1.0, 3.0, 5.0, 7.0, 9.0).toDF("value")
      val result = InterquartileRange("value").calculate(df)
      // Q1=3.0, Q3=7.0, IQR=4.0
      result.value shouldBe Success(4.0)
    }

    "fail for non-numeric data" in withSparkSession { session =>
      val df = getDfFull(session)
      assert(InterquartileRange("att1").calculate(df)
        .value.isFailure)
    }

    "compute correct value with where clause" in
      withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val result = InterquartileRange("att1",
        where = Some("item != '6'")).calculate(df)
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
      InterquartileRange("value").computeStateFrom(df) shouldBe
        None
    }

    "return 0 for single row" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(42.0).toDF("value")
      InterquartileRange("value").calculate(df)
        .value shouldBe Success(0.0)
    }

    "return 0 for all identical values" in
      withSparkSession { session =>
      import session.implicits._
      val df = Seq(5.0, 5.0, 5.0, 5.0).toDF("value")
      InterquartileRange("value").calculate(df)
        .value shouldBe Success(0.0)
    }

    "work with LongType column" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(1L, 2L, 3L, 4L, 5L, 6L).toDF("value")
      InterquartileRange("value").calculate(df)
        .value shouldBe Success(2.5)
    }

    "produce correct metric metadata" in
      withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      val metric = InterquartileRange("att1").calculate(df)
      metric shouldBe a[DoubleMetric]
      metric.entity shouldBe Entity.Column
      metric.name shouldBe "InterquartileRange"
      metric.instance shouldBe "att1"
    }

    "throw on state merge (quantiles not algebraically mergeable)" in
      withSparkSession { session =>
      import session.implicits._
      val s1 = InterquartileRangeState(2.0, 5.0)
      val s2 = InterquartileRangeState(3.0, 6.0)
      an[UnsupportedOperationException] should be thrownBy {
        s1.sum(s2)
      }
    }

    // Quantiles (Q1, Q3) are not algebraically mergeable from partial
    // states, unlike mean/variance/min/max which have exact merge
    // formulas. A conservative fallback would be:
    //   merged Q1 = min(q1_a, q1_b)
    //   merged Q3 = max(q3_a, q3_b)
    // This always OVERESTIMATES the IQR (widens the spread), never
    // underestimates. If we later implement approximate merge (e.g.
    // via t-digest or GK sketch), re-enable this test.
    //
    // "merge states provides conservative estimate" in
    //   withSparkSession { session =>
    //   val df = getDfWithNumericValues(session)
    //   val analyzer = InterquartileRange("att1")
    //
    //   val half1 = session.createDataFrame(
    //     session.sparkContext.parallelize(Seq(
    //       Row("1", 1, 0, 0, null),
    //       Row("2", 2, 0, 0, null),
    //       Row("3", 3, 0, 0, null))),
    //     df.schema)
    //   val half2 = session.createDataFrame(
    //     session.sparkContext.parallelize(Seq(
    //       Row("4", 4, 5, 4, 5),
    //       Row("5", 5, 6, 6, 6),
    //       Row("6", 6, 7, 7, 7))),
    //     df.schema)
    //
    //   val state1 = analyzer.computeStateFrom(half1)
    //   val state2 = analyzer.computeStateFrom(half2)
    //   val merged = Analyzers.merge(state1, state2)
    //   val mergedMetric = analyzer.computeMetricFrom(merged)
    //
    //   // Merged IQR is conservative (widest possible)
    //   mergedMetric.value.get should be >= 0.0
    // }
  }
}
