package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.State

case class JdbcFrequenciesAndNumRows(frequencies: Map[String, Long], numRows: Long)
  extends State[JdbcFrequenciesAndNumRows] {

  override def sum(other: JdbcFrequenciesAndNumRows): JdbcFrequenciesAndNumRows = {
    val totalRows = numRows + other.numRows
    val frequenciesSum = (frequencies.keySet ++ other.frequencies.keySet)
      .map {
        discreteValue => {
          val totalAbsolute = frequencies.getOrElse(discreteValue, 0L) +
                              other.frequencies.getOrElse(discreteValue, 0L)
          discreteValue -> totalAbsolute
        }
      }.toMap
    JdbcFrequenciesAndNumRows(frequenciesSum, totalRows)
  }
}
