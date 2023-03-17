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

  "Distinctness and CountDistinct" should {
    "have different behaviors with regard to nulls" in withSparkSession {session =>
      val data = getDfWithDistinctValues(session).select("att1")

      val numDistinct = data.distinct().count() // 4 including the null
      val numNonNullDistinct = data.na.drop().distinct().count() // 3 excluding the null

      val numberOfNonNullItems = data.na.drop().count() // 5

      val distinctnessValue = numNonNullDistinct.toDouble / numberOfNonNullItems

      val distinctness = new Check(CheckLevel.Error, "d1").hasDistinctness(Seq("att1"), almostEquals(_, distinctnessValue))
      val countDistinct = new Check(CheckLevel.Error, "d2").hasNumberOfDistinctValues("att1", _ == numDistinct)

      val suite = new VerificationSuite().onData(data).addCheck(distinctness).addCheck(countDistinct)
      val result = suite.run()
      assert(result.status == CheckStatus.Success)
    }
  }

  "DistinctValueCount" should {
    "return the number of distinct values in a column without doing a full count" in withSparkSession { session =>
      val data = getDfWithDistinctValues(session)
      val dvCount1 = new Check(CheckLevel.Error, "d1").hasNumberOfDistinctValues("att1", _ == 4.0)

      val result = new VerificationSuite().onData(data).addCheck(dvCount1).run()
      assert(result.status == CheckStatus.Success)

      val absoluteFrequencies = Map(
        "a" -> 2.0,
        "b" -> 2.0,
        "c" -> 1.0,
        Histogram.NullFieldReplacement -> 1.0
      )

      // result contains metrics for an absolute, not relative, frequencies
      result.metrics.foreach(pair => {
        val metric = pair._2.asInstanceOf[HistogramMetric]
        val distribution: Map[String, Double] = metric.value.get.values.map(v => v._1 -> v._2.ratio)
        assert(distribution == absoluteFrequencies)
      })
    }
  }

}
