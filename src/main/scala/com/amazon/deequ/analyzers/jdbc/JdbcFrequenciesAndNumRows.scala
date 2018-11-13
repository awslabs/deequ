package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.State

case class JdbcFrequenciesAndNumRows(frequencies: Map[String, Long], numRows: Long)
  extends State[JdbcFrequenciesAndNumRows] {

  override def sum(other: JdbcFrequenciesAndNumRows): JdbcFrequenciesAndNumRows = {
    val totalRows = numRows + other.numRows
    val frequenciesSum = (frequencies ++ other.frequencies).keySet.map {
      k => k -> (frequencies.getOrElse(k, 0) + other.frequencies.getOrElse(k, 0))
    }.toMap
    JdbcFrequenciesAndNumRows(frequenciesSum, totalRows)
  }
}
