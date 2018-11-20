package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.metrics.DoubleMetric

class JdbcEntropy(columns: Seq[String])
  extends JdbcScanShareableFrequencyBasedAnalyzer("Entropy", columns) {

  override def calculateMetricValue(state: JdbcFrequenciesAndNumRows): DoubleMetric = {
    val numRows = state.numRows
    val entropy = state.frequencies.values.map {
      frequency => -(frequency.toDouble / numRows) * math.log(frequency.toDouble / numRows)
    }.foldLeft(0.0)(_ + _)
    toSuccessMetric(entropy)
  }
}

object JdbcEntropy {
  def apply(column: String): JdbcEntropy = {
    new JdbcEntropy(column :: Nil)
  }
}
