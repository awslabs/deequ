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
import com.amazon.deequ.analyzers.MeanState
import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.postgresql.util.PSQLException
import Preconditions.{hasTable, hasColumn, isNumeric}

case class JdbcMean(column: String, where: Option[String] = None)
  extends JdbcAnalyzer[MeanState, DoubleMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable() :: hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def computeStateFrom(table: Table): Option[MeanState] = {

    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | SUM($column) AS col_sum,
         | COUNT(*) AS col_count
         |FROM
         | ${table.name}
         |WHERE
         | ${where.getOrElse("TRUE=TRUE")}
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    if (result.next()) {
      val col_count = result.getLong("col_count")
      val col_sum = result.getDouble("col_sum")

      if (!result.wasNull()) {
        return Some(MeanState(col_sum, col_count))
      }
    }
    None
  }

  override def computeMetricFrom(state: Option[MeanState]): DoubleMetric = {
    state match {
      case Some(theState) =>
        metricFromValue(theState.metricValue(), "Mean", column, Entity.Column)
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcMean, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    metricFromFailure(failure, "Mean", column, Entity.Column)
  }
}
