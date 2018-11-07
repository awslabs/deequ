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

import com.amazon.deequ.analyzers.Analyzers.{metricFromFailure, metricFromValue}
import com.amazon.deequ.analyzers.StandardDeviationState
import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.postgresql.util.PSQLException
import Preconditions.{hasColumn, hasTable, isNumeric}

case class JdbcStandardDeviation(column: String)
  extends JdbcAnalyzer[StandardDeviationState, DoubleMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable(column) :: hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def computeStateFrom(table: Table): Option[StandardDeviationState] = {

    val connection = table.jdbcConnection
    val query =
      s"""
         |SELECT
         | col_count,
         | col_avg,
         | SUM(POWER($column - col_avg, 2)) AS col_m2
         |FROM
         | (SELECT
         |  $column
         | FROM
         |  ${table.name}) AS A
         |CROSS JOIN
         | (SELECT
         |   COUNT($column) AS col_count,
         |   AVG($column) AS col_avg
         |  FROM ${table.name}) AS B
         |GROUP BY
         | col_count,
         | col_avg
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    try {
      result.next()
    }
    catch {
      case error: PSQLException => throw error
    }

    try {
      val col_count = result.getDouble("col_count")
      val col_avg = result.getDouble("col_avg")
      val col_m2 = result.getDouble("col_m2")

      Some(StandardDeviationState(col_count, col_avg, col_m2))
    }
    catch {
      case error: Exception => throw error
    }
  }

  override def computeMetricFrom(state: Option[StandardDeviationState]): DoubleMetric = {
    state match {
      case Some(theState) =>
        metricFromValue(theState.metricValue(), "StandardDeviation", column, Entity.Column)
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcStandardDeviation, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    metricFromFailure(failure, "StandardDeviation", column, Entity.Column)
  }
}
