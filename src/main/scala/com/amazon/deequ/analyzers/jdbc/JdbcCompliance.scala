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
import com.amazon.deequ.analyzers.NumMatchesAndCount
import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasTable, hasNoInjection}
import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.{DoubleMetric, Entity}

/**
  * Compliance is a measure of the fraction of rows that complies with the given column constraint.
  * E.g if the constraint is "att1>3" and data frame has 5 rows with att1 column value greater than
  * 3 and 10 rows under 3; a DoubleMetric would be returned with 0.33 value
  *
  * @param instance         Unlike other column analyzers (e.g completeness) this analyzer can not
  *                         infer to the metric instance name from column name.
  *                         Also the constraint given here can be referring to multiple columns,
  *                         so metric instance name should be provided,
  *                         describing what the analysis being done for.
  * @param predicate        SQL-predicate to apply per row
  * @param where            Additional filter to apply before the analyzer is run.
  */

case class JdbcCompliance(instance: String, predicate: String, where: Option[String] = None)
  extends JdbcAnalyzer[NumMatchesAndCount, DoubleMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable() :: hasNoInjection(Option(predicate)) :: hasNoInjection(where) :: Nil
  }

  override def computeStateFrom(table: Table): Option[NumMatchesAndCount] = {

    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | SUM(CASE WHEN $predicate THEN 1 ELSE 0 END) AS num_matches,
         | COUNT(*) AS num_rows
         |FROM
         | ${table.name}
         |WHERE
         |  ${where.getOrElse("TRUE=TRUE")}
       """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    if (result.next()) {
      val num_matches = result.getLong("num_matches")
      val num_rows = result.getLong("num_rows")

      if (num_rows > 0) {
        result.close()
        return Some(NumMatchesAndCount(num_matches, num_rows))
      }
    }
    result.close()
    None
  }

  override def computeMetricFrom(state: Option[NumMatchesAndCount]): DoubleMetric = {
    state match {
      case Some(theState) =>
        metricFromValue(theState.metricValue(), "Compliance", instance, Entity.Column)
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcCompliance, all input values were Null"))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception): DoubleMetric = {
    metricFromFailure(failure, "Compliance", instance, Entity.Column)
  }
}
