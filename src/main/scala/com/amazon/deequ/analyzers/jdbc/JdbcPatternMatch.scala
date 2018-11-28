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
import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, hasTable, hasNoInjection}
import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.{DoubleMetric, Entity}

import scala.util.matching.Regex

/**
  * PatternMatch is a measure of the fraction of rows that complies with a given
  * column regex constraint. E.g if the constraint is Patterns.CREDITCARD and the
  * data frame has 5 rows which contain a credit card number in a certain column
  * according to the regex and 10 rows that do not, a DoubleMetric would be
  * returned with 0.33 as value
  *
  * @param column     Column to do the pattern match analysis on
  * @param pattern    The regular expression to check for
  * @param where      Additional filter to apply before the analyzer is run.
  */
case class JdbcPatternMatch(column: String, pattern: Regex, where: Option[String] = None)
  extends JdbcAnalyzer[NumMatchesAndCount, DoubleMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable() :: hasColumn(column) :: hasNoInjection(where) :: Nil
  }

  override def computeStateFrom(table: Table): Option[NumMatchesAndCount] = {

    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | SUM(CASE WHEN
         | (SELECT regexp_matches(CAST($column AS text), ?, '')) IS NOT NULL
         | THEN 1 ELSE 0 END) as num_matches,
         | COUNT(*) AS num_rows
         |FROM
         | ${table.name}
         |WHERE
         | ${where.getOrElse("TRUE = TRUE")};
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    statement.setString(1, pattern.toString())

    val result = statement.executeQuery()

    if (result.next()) {
      val num_matches = result.getLong("num_matches")
      val num_rows = result.getLong("num_rows")
      result.close()
      Some(NumMatchesAndCount(num_matches, num_rows))
    } else {
      result.close()
      None
    }
  }

  override def computeMetricFrom(state: Option[NumMatchesAndCount]): DoubleMetric = {
    state match {
      case Some(theState) =>
        metricFromValue(theState.metricValue(), "PatternMatch", column, Entity.Column)
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcPatternMatch, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    metricFromFailure(failure, "PatternMatch", column, Entity.Column)
  }
}
