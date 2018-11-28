package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers._
import com.amazon.deequ.utils.TempFileUtils
import org.scalatest.{Matchers, WordSpec}

class JdbcStateProviderTest
  extends WordSpec with Matchers with JdbcContextSpec with JdbcFixtureSupport {

  "Analyzers" should {

    "correctly restore their state from memory" in withJdbc { connection =>

      val provider = JdbcInMemoryStateProvider()

      val data = getTableWithPricedItems(connection)

      assertCorrectlyRestoresState[NumMatches](provider, provider, JdbcSize(), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        JdbcCompleteness("att1"), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        JdbcCompliance("att1", "att1 = 'b'"), data)

      assertCorrectlyRestoresState[SumState](provider, provider, JdbcSum("price"), data)
      assertCorrectlyRestoresState[MeanState](provider, provider, JdbcMean("price"), data)
      assertCorrectlyRestoresState[MinState](provider, provider, JdbcMinimum("price"), data)
      assertCorrectlyRestoresState[MaxState](provider, provider, JdbcMaximum("price"), data)
      assertCorrectlyRestoresState[StandardDeviationState](provider, provider,
        JdbcStandardDeviation("price"), data)

      assertCorrectlyRestoresState[CorrelationState](provider, provider,
        JdbcCorrelation("count", "price"), data)

      assertCorrectlyRestoresFrequencyBasedState(provider, provider, JdbcUniqueness("att1"), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider,
        JdbcUniqueness(Seq("att1", "count")), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider, JdbcEntropy("att1"), data)
    }

    "correctly restore their state from the filesystem" in withJdbc { connection =>

      val tempDir: String = TempFileUtils.tempDir("stateRestoration")

      val provider = JdbcFileSystemStateProvider(tempDir)

      val data = getTableWithPricedItems(connection)

      assertCorrectlyRestoresState[NumMatches](provider, provider, JdbcSize(), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        JdbcCompleteness("att1"), data)
      assertCorrectlyRestoresState[NumMatchesAndCount](provider, provider,
        JdbcCompliance("att1", "att1 = 'b'"), data)

      assertCorrectlyRestoresState[SumState](provider, provider, JdbcSum("price"), data)
      assertCorrectlyRestoresState[MeanState](provider, provider, JdbcMean("price"), data)
      assertCorrectlyRestoresState[MinState](provider, provider, JdbcMinimum("price"), data)
      assertCorrectlyRestoresState[MaxState](provider, provider, JdbcMaximum("price"), data)
      assertCorrectlyRestoresState[StandardDeviationState](provider, provider,
        JdbcStandardDeviation("price"), data)

      assertCorrectlyRestoresState[CorrelationState](provider, provider,
        JdbcCorrelation("count", "price"), data)

      assertCorrectlyRestoresFrequencyBasedState(provider, provider, JdbcUniqueness("att1"), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider,
        JdbcUniqueness(Seq("att1", "count")), data)
      assertCorrectlyRestoresFrequencyBasedState(provider, provider, JdbcEntropy("att1"), data)
    }
  }

  def assertCorrectlyRestoresState[S <: State[S]](
         persister: JdbcStatePersister,
         loader: JdbcStateLoader,
         analyzer: JdbcAnalyzer[S, _],
         table: Table) {

    val stateResult = analyzer.computeStateFrom(table)
    assert(stateResult.isDefined)
    val state = stateResult.get

    persister.persist[S](analyzer, state)
    val clonedState = loader.load[S](analyzer)

    assert(clonedState.isDefined)
    assert(state == clonedState.get)
  }

  def assertCorrectlyRestoresFrequencyBasedState(
          persister: JdbcStatePersister,
          loader: JdbcStateLoader,
          analyzer: JdbcAnalyzer[JdbcFrequenciesAndNumRows, _],
          table: Table) {

    val stateResult = analyzer.computeStateFrom(table)
    assert(stateResult.isDefined)
    val state = stateResult.get

    persister.persist[JdbcFrequenciesAndNumRows](analyzer, state)
    val clonedState = loader.load[JdbcFrequenciesAndNumRows](analyzer)

    assert(clonedState.isDefined)
    assert(state.numRows == clonedState.get.numRows)
    assert(state.frequencies == clonedState.get.frequencies)
  }

}
