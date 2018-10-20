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
import com.amazon.deequ.utils.{FixtureSupport, TempFileUtils}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{Matchers, WordSpec}

class StateProviderTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Analyzers" should {

    "correctly restore their state from memory" in withSparkSession { session =>

      val provider = InMemoryStateProvider()

      val data = someData(session)

      assertCorrectlyRestoresState[NumMatches](provider, provider, Size(), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        Completeness("att1"), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        Compliance("att1", "att1 = 'b'"), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        PatternMatch("att1", Patterns.EMAIL), data)

      assertCorrectlyRestoresState[SumState](provider, provider, Sum("price"), data)
      assertCorrectlyRestoresState[MeanState](provider, provider, Mean("price"), data)
      assertCorrectlyRestoresState[MinState](provider, provider, Minimum("price"), data)
      assertCorrectlyRestoresState[MaxState](provider, provider, Maximum("price"), data)
      assertCorrectlyRestoresState[StandardDeviationState](provider, provider,
        StandardDeviation("price"), data)

      assertCorrectlyRestoresState[DataTypeHistogram](provider, provider, DataType("item"), data)
      assertCorrectlyRestoresStateForHLL(provider, provider, ApproxCountDistinct("att1"), data)
      assertCorrectlyRestoresState[CorrelationState](provider, provider,
        Correlation("count", "price"), data)

      assertCorrectlyRestoresFrequencyBasedState(provider, provider, Uniqueness("att1"), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider,
        Uniqueness(Seq("att1", "count")), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider, Entropy("att1"), data)

      assertCorrectlyApproxQuantileState(provider, provider, ApproxQuantile("price", 0.5), data)
    }

    "correctly restore their state from the filesystem" in withSparkSession { session =>

      val tempDir = TempFileUtils.tempDir("stateRestoration")

      val provider = HdfsStateProvider(session, tempDir)

      val data = someData(session)

      assertCorrectlyRestoresState[NumMatches](provider, provider, Size(), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        Completeness("att1"), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        Compliance("att1", "att1 = 'b'"), data)

      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        PatternMatch("att1", Patterns.EMAIL), data)

      assertCorrectlyRestoresState[SumState](provider, provider, Sum("price"), data)
      assertCorrectlyRestoresState[MeanState](provider, provider, Mean("price"), data)
      assertCorrectlyRestoresState[MinState](provider, provider, Minimum("price"), data)
      assertCorrectlyRestoresState[MaxState](provider, provider, Maximum("price"), data)
      assertCorrectlyRestoresState[StandardDeviationState](provider, provider,
        StandardDeviation("price"), data)

      assertCorrectlyRestoresState[DataTypeHistogram](provider, provider, DataType("item"), data)
      assertCorrectlyRestoresStateForHLL(provider, provider, ApproxCountDistinct("att1"), data)
      assertCorrectlyRestoresState[CorrelationState](provider, provider,
        Correlation("count", "price"), data)

      assertCorrectlyRestoresFrequencyBasedState(provider, provider, Uniqueness("att1"), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider,
        Uniqueness(Seq("att1", "count")), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider, Entropy("att1"), data)

      assertCorrectlyApproxQuantileState(provider, provider, ApproxQuantile("price", 0.5), data)
    }
  }

  def assertCorrectlyRestoresState[S <: State[S]](
      persister: StatePersister,
      loader: StateLoader,
      analyzer: Analyzer[S, _],
      data: DataFrame) {

    val stateResult = analyzer.computeStateFrom(data)
    assert(stateResult.isDefined)
    val state = stateResult.get

    persister.persist[S](analyzer, state)
    val clonedState = loader.load[S](analyzer)

    assert(clonedState.isDefined)
    assert(state == clonedState.get)
  }

  def assertCorrectlyApproxQuantileState(
      persister: StatePersister,
      loader: StateLoader,
      analyzer: Analyzer[ApproxQuantileState, _],
      data: DataFrame) {

    val stateResult = analyzer.computeStateFrom(data)
    assert(stateResult.isDefined)

    val state = stateResult.get

    persister.persist[ApproxQuantileState](analyzer, state)
    val clonedState = loader.load[ApproxQuantileState](analyzer)

    assert(clonedState.isDefined)
    val summary = state.percentileDigest.quantileSummaries
    val clonedSummary = clonedState.get.percentileDigest.quantileSummaries

    assert(summary.compressThreshold == clonedSummary.compressThreshold)
    assert(summary.relativeError == clonedSummary.relativeError)
    assert(summary.count == clonedSummary.count)
    assert(summary.sampled.sameElements(clonedSummary.sampled))
  }

  def assertCorrectlyRestoresStateForHLL(
      persister: StatePersister,
      loader: StateLoader,
      analyzer: Analyzer[ApproxCountDistinctState, _],
      data: DataFrame) {

    val stateResult = analyzer.computeStateFrom(data)
    assert(stateResult.isDefined)
    val state = stateResult.get

    persister.persist[ApproxCountDistinctState](analyzer, state)
    val clonedState = loader.load[ApproxCountDistinctState](analyzer)

    assert(clonedState.isDefined)
    assert(state.words.sameElements(clonedState.get.words))
  }

  def assertCorrectlyRestoresFrequencyBasedState(
      persister: StatePersister,
      loader: StateLoader,
      analyzer: Analyzer[FrequenciesAndNumRows, _],
      data: DataFrame) {

    val stateResult = analyzer.computeStateFrom(data)
    assert(stateResult.isDefined)
    val state = stateResult.get

    persister.persist[FrequenciesAndNumRows](analyzer, state)
    val clonedState = loader.load[FrequenciesAndNumRows](analyzer)

    assert(clonedState.isDefined)
    assert(state.numRows == clonedState.get.numRows)
    assert(state.frequencies.collect().toSet == clonedState.get.frequencies.collect().toSet)
  }

  def someData(session: SparkSession): DataFrame = {
    import session.implicits._

    Seq(
      ("1", "a", 17, 1.3),
      ("2", null, 12, 76.0),
      ("3", "b", 15, 89.0),
      ("4", "b", 12, 12.7),
      ("5", null, 1, 1.0),
      ("6", "a", 21, 78.0),
      ("7", null, 12, 0.0))
    .toDF("item", "att1", "count", "price")
  }

}
