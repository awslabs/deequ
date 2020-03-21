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
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.utils.{FixtureSupport, TempFileUtils}
import org.apache.spark.sql.{DataFrame, SparkSession, AnalysisException}
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

      assertCorrectlyRestoresState[MaxState](provider, provider, MaxLength("att1"), data)
      assertCorrectlyRestoresState[MinState](provider, provider, MinLength("att1"), data)

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

      assertCorrectlyRestoresState[MaxState](provider, provider, MaxLength("att1"), data)
      assertCorrectlyRestoresState[MinState](provider, provider, MinLength("att1"), data)

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

    "store frequency based state to same HDFS StateProvider for same analyzer more than once " +
      "should fail if allowOverwrite is not set to true" in withSparkSession { session =>

      val tempDir = TempFileUtils.tempDir("statePersist")

      val provider = HdfsStateProvider(session, tempDir)

      val data = someData(session)

      val uniquenessAtt1Analyzer = Uniqueness("att1")
      val entropyAtt1Analyzer = Entropy("att1")
      val uniquenessAtt1AndCountAnalyzer = Uniqueness(Seq("att1", "count"))

      val uniquenessAtt1State = uniquenessAtt1Analyzer.computeStateFrom(data).get
      val entropyAtt1State = entropyAtt1Analyzer.computeStateFrom(data).get
      val uniquenessAtt1AndCountState = uniquenessAtt1AndCountAnalyzer.computeStateFrom(data).get

      provider.persist(uniquenessAtt1Analyzer, uniquenessAtt1State)
      provider.persist(entropyAtt1Analyzer, entropyAtt1State)
      provider.persist(uniquenessAtt1AndCountAnalyzer, uniquenessAtt1AndCountState)

      val caught = intercept[AnalysisException]{
        provider.persist(uniquenessAtt1Analyzer, uniquenessAtt1State)
      }
      assert(caught.message.contains("already exists"))
    }

    "store frequency based state to same HDFS StateProvider for same analyzer more than once " +
      "should overwrite state if allowOverwrite is set to true" in withSparkSession { session =>

      val tempDir = TempFileUtils.tempDir("statePersist")

      val provider = HdfsStateProvider(session, tempDir, allowOverwrite = true)

      val data = someData(session)
      val filteredData = data.filter("att1 == 'a'")

      val uniquenessAtt1Analyzer = Uniqueness("att1")

      assertCorrectlyRestoresFrequencyBasedState(provider, provider, uniquenessAtt1Analyzer, data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider, uniquenessAtt1Analyzer,
        filteredData)
    }

    "should store Histogram result to InMemoryStateProvider" in withSparkSession { session =>

      val provider = InMemoryStateProvider()

      val data = someData(session)
      val histogramCountAnalyzer = Histogram("count")

      val analysis = Analysis().addAnalyzer(histogramCountAnalyzer)

      AnalysisRunner.run(
        data = data,
        analysis = analysis,
        saveStatesWith = Some(provider))

      assert(provider.load(histogramCountAnalyzer).isDefined)
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
