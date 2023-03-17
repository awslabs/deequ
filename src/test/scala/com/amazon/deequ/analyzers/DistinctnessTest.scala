/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.Constraint
import com.amazon.deequ.dataFrameWithColumn
import com.amazon.deequ.metrics.Distribution
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.HistogramMetric
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DistinctnessTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  def almostEquals(a: Double, b: Double, e: Double): Boolean = {
    (a-b).abs < e
  }

  def almostEquals(a: Double, b: Double): Boolean = almostEquals(a, b, 0.01)

  "Distinctness" should {
    "return the ratio of distinct values in a column" in withSparkSession {session =>
      val data = getDfWithDistinctValues(session)
      val distinctAtt1 = new Check(CheckLevel.Error, "d1").hasDistinctness(Seq("att1"), almostEquals(_, 0.6))
      val distinctAtt2 = new Check(CheckLevel.Error, "d2").hasDistinctness(Seq("att2"), almostEquals(_, 0.5))

      val suite = new VerificationSuite().onData(data).addCheck(distinctAtt1).addCheck(distinctAtt2)
      val result = suite.run()
      assert(result.status == CheckStatus.Success)

      val metrics = result.metrics
      metrics.foreach(m => {
        val metric: Metric[_] = m._2
        metric match {
          case d: DoubleMetric => assert(d.value.get == 0.6 | d.value.get == 0.5)
          case _ => fail("Metric is not a Double")
        }
      })
    }
  }

  "DistinctValueCount" should {
    "return the number of distinct values in a column without doing a full count" in withSparkSession { session =>
      val data = getDfWithDistinctValues(session)
      val dvCount1 = new Check(CheckLevel.Error, "d1").hasNumberOfDistinctValues("att1", _ == 4.0)
      val dvCount2 = new Check(CheckLevel.Error, "d2").hasNumberOfDistinctValues("att2", _ == 3.0)

      val suite = new VerificationSuite().onData(data).addCheck(dvCount1).addCheck(dvCount2)
      val result = suite.run()
      assert(result.status == CheckStatus.Success)
    }
  }

}
