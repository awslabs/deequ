package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.State
import com.amazon.deequ.metrics.Distribution

case class JdbcFrequenciesAndNumRows(frequencies: Distribution, numRows: Long)
  extends State[JdbcFrequenciesAndNumRows] {

  override def sum(other: JdbcFrequenciesAndNumRows): JdbcFrequenciesAndNumRows = {
    val totalRows = numRows + other.numRows
    val frequenciesSum = frequencies.sum(other.frequencies, totalRows)

    JdbcFrequenciesAndNumRows(frequenciesSum, totalRows)
  }
}