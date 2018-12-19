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

package com.amazon.deequ.analyzers.jdbc

import java.sql.ResultSet

import JdbcAnalyzers._
import Preconditions._
import com.amazon.deequ.metrics.DoubleMetric

/** Base class for all analyzers that operate the frequencies of groups in the data */
abstract class JdbcFrequencyBasedAnalyzer(columnsToGroupOn: Seq[String])
  extends JdbcAnalyzer[JdbcFrequenciesAndNumRows, DoubleMetric] {

  def groupingColumns(): Seq[String] = { columnsToGroupOn }

  override def computeStateFrom(table: Table): Option[JdbcFrequenciesAndNumRows] = {
    Some(JdbcFrequencyBasedAnalyzer.computeFrequencies(table, groupingColumns()))
  }

  /** We need at least one grouping column, and all specified columns must exist */
  override def preconditions: Seq[Table => Unit] = {
    Seq(hasTable()) ++ Seq(atLeastOne(columnsToGroupOn)) ++
      columnsToGroupOn.map { hasColumn } ++ super.preconditions
  }
}

object JdbcFrequencyBasedAnalyzer {

  /** Compute the frequencies of groups in the data, essentially via a query like
    *
    * SELECT colA, colB, ..., COUNT(*)
    * FROM DATA
    * WHERE colA IS NOT NULL AND colB IS NOT NULL AND ...
    * GROUP BY colA, colB, ...
    */
  def computeFrequencies(
    table: Table,
    groupingColumns: Seq[String],
    numRows: Option[Long] = None)
  : JdbcFrequenciesAndNumRows = {

    val connection = table.jdbcConnection

    val columns = groupingColumns.mkString("", ", ", "")
    val query =
      s"""
         | SELECT $columns, COUNT(*) AS absolute
         |    FROM ${table.name}
         |    GROUP BY $columns
        """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    def convertResultSet(resultSet: ResultSet,
                      map: Map[Seq[String], Long],
                      total: Long): (Map[Seq[String], Long], Long) = {
      if (result.next()) {
        var columnValues = Seq[String]()
        val absolute = result.getLong("absolute")

        // only make a map entry if the value is defined for all columns
        for ( i <- 1 to groupingColumns.size) {
          val columnValue = Option(result.getObject(i))
          columnValue match {
            case Some(theColumnValue) =>
              columnValues = columnValues :+ theColumnValue.toString
            case None =>
              return convertResultSet(result, map, total + absolute)
          }
        }

        val entry = columnValues -> absolute
        convertResultSet(result, map + entry, total + absolute)
      } else {
        (map, total)
      }
    }
    val (frequencies, numRows) = convertResultSet(result, Map[Seq[String], Long](), 0)
    result.close()

    JdbcFrequenciesAndNumRows(frequencies, numRows)
  }
}

/** Base class for all analyzers that compute a (shareable) aggregation over the grouped data */
abstract class JdbcScanShareableFrequencyBasedAnalyzer(name: String, columnsToGroupOn: Seq[String])
  extends JdbcFrequencyBasedAnalyzer(columnsToGroupOn) {

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>
        calculateMetricValue(theState)
      case None =>
        metricFromEmpty(this, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
    }
  }

  override private[deequ] def toFailureMetric(exception: Exception): DoubleMetric = {
    metricFromFailure(exception, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
  }

  protected def toSuccessMetric(value: Double): DoubleMetric = {
    metricFromValue(value, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
  }

  def calculateMetricValue(state: JdbcFrequenciesAndNumRows): DoubleMetric
}
