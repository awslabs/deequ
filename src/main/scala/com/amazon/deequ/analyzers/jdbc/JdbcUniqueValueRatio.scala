package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.metrics.DoubleMetric

class JdbcUniqueValueRatio(columns: Seq[String])
  extends JdbcScanShareableFrequencyBasedAnalyzer("UniqueValueRatio", columns) {

  override def calculateMetricValue(state: JdbcFrequenciesAndNumRows): DoubleMetric = {
    val numUniqueValues = state.frequencies.values.count(_ == 1)
    val numDistinctValues = state.frequencies.size
    toSuccessMetric(numUniqueValues.toDouble / numDistinctValues)
  }
}

object JdbcUniqueValueRatio {
  def apply(column: String): JdbcUniqueValueRatio = {
    new JdbcUniqueValueRatio(column :: Nil)
  }
}
