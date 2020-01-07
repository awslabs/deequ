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
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success


class IncrementalAnalyzerTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "Size analyzer" should {
    "compute correct metrics" in withSparkSession { session =>

      val analyzer = Size()

      val initial = initialData(session)
      val delta = deltaData(session)

      val initialState = analyzer.computeStateFrom(initial)
      val initialSize = analyzer.computeMetricFrom(initialState)

      val deltaState = analyzer.computeStateFrom(delta)
      val deltaSize = analyzer.computeMetricFrom(deltaState)

      val mergedState = Analyzers.merge(initialState, deltaState)

      val size = analyzer.computeMetricFrom(mergedState)

      assert(initialSize.value == Success(3.0))
      assert(deltaSize.value == Success(2.0))
      assert(size.value == Success(5.0))
    }
  }

  "ComplianceAnalyzer" should {
    "compute correct metrics" in withSparkSession { session =>

      val analyzer = Compliance("att1", "att1 = 'b'")

      val initial = initialData(session)
      val delta = deltaData(session)

      val initialState = analyzer.computeStateFrom(initial)
      val initialCompliance = analyzer.computeMetricFrom(initialState)

      val deltaState = analyzer.computeStateFrom(delta)
      val deltaCompliance = analyzer.computeMetricFrom(deltaState)

      val mergedState = Analyzers.merge(initialState, deltaState)

      val compliance = analyzer.computeMetricFrom(mergedState)

      assert(initialCompliance.value == Success(0.3333333333333333))
      assert(deltaCompliance.value == Success(0.5))
      assert(compliance.value == Success(0.4))
    }
  }

  "CompletenessAnalyzer" should {
    "compute correct metrics" in withSparkSession { session =>

      val analyzer = Completeness("att1")

      val initial = initialData(session)
      val delta = deltaData(session)

      val initialState = analyzer.computeStateFrom(initial)
      val initialCompleteness = analyzer.computeMetricFrom(initialState)

      val deltaState = analyzer.computeStateFrom(delta)
      val deltaCompleteness = analyzer.computeMetricFrom(deltaState)

      val mergedState = Analyzers.merge(initialState, deltaState)

      val completeness = analyzer.computeMetricFrom(mergedState)

      assert(initialCompleteness.value == Success(0.6666666666666666))
      assert(deltaCompleteness.value == Success(0.5))
      assert(completeness.value == Success(0.6))
    }
  }

  "UniquenessAnalyzer" should {
    "compute correct metrics for a single column" in withSparkSession { session =>

      val analyzer = Uniqueness("att1")

      val initial = initialData(session)
      val delta = deltaData(session)

      val initialState = analyzer.computeStateFrom(initial)
      val initialUniqueness = analyzer.computeMetricFrom(initialState)

      val deltaState = analyzer.computeStateFrom(delta)
      val deltaUniqueness = analyzer.computeMetricFrom(deltaState)

      val mergedState = Analyzers.merge(initialState, deltaState)

      val uniqueness = analyzer.computeMetricFrom(mergedState)

      assert(initialUniqueness.value == Success(1.0))
      assert(deltaUniqueness.value == Success(1.0))
      assert(uniqueness.value == Success(1.0 / 3))
    }

    "compute correct metrics for column combinations" in withSparkSession { session =>

      val analyzer = Uniqueness(Seq("att1", "count"))

      val initial = initialData(session)
      val delta = deltaData(session)

      val initialState = analyzer.computeStateFrom(initial)
      val initialUniqueness = analyzer.computeMetricFrom(initialState)

      val deltaState = analyzer.computeStateFrom(delta)
      val deltaUniqueness = analyzer.computeMetricFrom(deltaState)

      val mergedState = Analyzers.merge(initialState, deltaState)

      val uniqueness = analyzer.computeMetricFrom(mergedState)

      assert(initialUniqueness.value == Success(1.0))
      assert(deltaUniqueness.value == Success(1.0))
      assert(uniqueness.value == Success(0.2))
    }
  }

  "EntropyAnalyzer" should {
    "compute correct metrics" in withSparkSession { session =>

      val analyzer = Entropy("att1")

      val initial = initialData(session)
      val delta = deltaData(session)

      val initialState = analyzer.computeStateFrom(initial)
      val initialEntropy = analyzer.computeMetricFrom(initialState)

      val deltaState = analyzer.computeStateFrom(delta)
      val deltaEntropy = analyzer.computeMetricFrom(deltaState)

      val mergedState = Analyzers.merge(initialState, deltaState)
      val entropy = analyzer.computeMetricFrom(mergedState)

      val expectedInitialEntropy = analyzer.calculate(initial)
      val expectedDeltaEntropy = analyzer.calculate(delta)
      val expectedEntropy = analyzer.calculate(initial.union(delta))

      assert(initialEntropy == expectedInitialEntropy)
      assert(deltaEntropy == expectedDeltaEntropy)
      assert(entropy == expectedEntropy)
    }
  }

  "ApproxQuantile Analyzer" should {
    "compute correct metrics for the whole and partial data-sets" in withSparkSession { session =>
      import session.implicits._

      val attribute = "att1"
      val first = Seq(("1", 0.0), ("2", 1.0), ("3", 2.0)).toDF("item", attribute)
      val second = Seq(("1", -2.0), ("2", -1.0)).toDF("item", attribute)
      val firstAndSecond = first.union(second)

      val analyzer = ApproxQuantile(attribute, quantile = 0.5)

      val firstState = analyzer.computeStateFrom(first)
      val secondState = analyzer.computeStateFrom(second)

      val mergedState = Analyzers.merge(firstState, secondState)

      val firstAndSecondSummedMetric = analyzer.computeMetricFrom(mergedState)
        .value

      val firstAndSecondMetric = analyzer.calculate(firstAndSecond).value

      firstAndSecondMetric shouldBe Success(0.0)
      firstAndSecondSummedMetric shouldBe firstAndSecondMetric
    }
  }

  "EntropyAnalyzer" should {
    "compute correct metrics for three snapshots" in withSparkSession { session =>

      val analyzer = Entropy("att1")

      val delta1 = initialData(session)
      val delta2 = deltaData(session)
      val delta3 = moreDeltaData(session)

      val d1 = delta1
      val d2 = delta1.union(delta2)
      val d3 = delta1.union(delta2).union(delta3)

      // non-incremental computation
      val entropyOfD1 = analyzer.calculate(d1)
      val entropyOfD2 = analyzer.calculate(d2)
      val entropyOfD3 = analyzer.calculate(d3)

      // incremental computation from deltas only
      val stateOfD1 = analyzer.computeStateFrom(delta1)
      val incrementalEntropyOfD1 = analyzer.computeMetricFrom(stateOfD1)

      val stateOfD2 = analyzer.computeStateFrom(delta2)

      val stateOfD1AndD2 = Analyzers.merge(stateOfD1, stateOfD2)

      val incrementalEntropyOfD2 = analyzer.computeMetricFrom(stateOfD1AndD2)

      val stateOfD3 = analyzer.computeStateFrom(delta3)

      val stateOfD1AndD2AndD3 = Analyzers.merge(stateOfD1AndD2, stateOfD3)

      val incrementalEntropyOfD3 =
        analyzer.computeMetricFrom(stateOfD1AndD2AndD3)

      assert(entropyOfD1 == incrementalEntropyOfD1)
      assert(entropyOfD2 == incrementalEntropyOfD2)
      assert(entropyOfD3 == incrementalEntropyOfD3)
    }
  }

  def initialData(session: SparkSession): DataFrame = {
    import session.implicits._

    Seq(
      ("1", "a", 12),
      ("2", null, 12),
      ("3", "b", 12))
      .toDF("item", "att1", "count")
  }

  def deltaData(session: SparkSession): DataFrame = {
    import session.implicits._

    Seq(
      ("4", "b", 12),
      ("5", null, 12))
      .toDF("item", "att1", "count")
  }

  def moreDeltaData(session: SparkSession): DataFrame = {
    import session.implicits._

    Seq(
      ("6", "a", 12),
      ("7", null, 12))
      .toDF("item", "att1", "count")
  }
}
