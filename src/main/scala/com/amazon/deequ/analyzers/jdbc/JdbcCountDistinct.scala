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
import com.amazon.deequ.analyzers.NumMatches
import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.postgresql.util.PSQLException

case class JdbcCountDistinct(column: String)
  extends JdbcAnalyzer[NumMatches, DoubleMetric] {

  override def computeStateFrom(table: Table): Option[NumMatches] = {

    val connection = table.jdbcConnection

    validateParams(table, column)

    val query =
      s"""
         |SELECT
         | COUNT(DISTINCT $column) AS count_distinct
         |FROM
         | ${table.name}
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
      val col_count_distinct = result.getLong("count_distinct")

      Some(NumMatches(col_count_distinct))
    }
    catch {
      case error: Exception => throw error
    }
  }

  override def computeMetricFrom(state: Option[NumMatches]): DoubleMetric = {
    state match {
      case Some(theState) =>
        metricFromValue(theState.metricValue(), "CountDistinct", column, Entity.Column)
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcCountDistinct, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    metricFromFailure(failure, "CountDistinct", column, Entity.Column)
  }
}
