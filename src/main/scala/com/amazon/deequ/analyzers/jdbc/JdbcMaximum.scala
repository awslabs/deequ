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
import com.amazon.deequ.analyzers.MaxState
import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.postgresql.util.PSQLException
import Preconditions._

case class JdbcMaximum(column: String, where: Option[String] = None)
  extends JdbcAnalyzer[MaxState, DoubleMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable() :: hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def computeStateFrom(table: Table): Option[MaxState] = {

    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | MAX($column) AS col_max
         |FROM
         | ${table.name}
         |WHERE
         | ${where.getOrElse("TRUE=TRUE")}
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    if (result.next()) {
      val col_max = result.getDouble("col_max")

      if (!result.wasNull()) {
        return Some(MaxState(col_max))
      }
    }
    None
  }

  override def computeMetricFrom(state: Option[MaxState]): DoubleMetric = {
    state match {
      case Some(theState) =>
        metricFromValue(theState.metricValue(), "Maximum", column, Entity.Column)
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcMaximum, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    metricFromFailure(failure, "Maximum", column, Entity.Column)
  }
}
