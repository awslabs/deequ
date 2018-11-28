/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  * http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ.analyzers.jdbc

import java.sql.Types._

import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasTable, hasColumn, hasNoInjection}
import com.amazon.deequ.analyzers.runners.{EmptyStateException, MetricCalculationException}
import com.amazon.deequ.analyzers.{DataTypeHistogram, DataTypeInstances}
import com.amazon.deequ.metrics.HistogramMetric

import scala.util.{Failure, Success}

case class JdbcDataType(column: String,
                        where: Option[String] = None)
  extends JdbcAnalyzer[DataTypeHistogram, HistogramMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable() :: hasColumn(column) :: hasNoInjection(where) :: Nil
  }

  override def computeStateFrom(table: Table): Option[DataTypeHistogram] = {

    val connection = table.jdbcConnection

    val dataType = Preconditions.getColumnDataType(table, column) match {
      case BIGINT | INTEGER | TINYINT | SMALLINT => DataTypeInstances.Integral
      case BOOLEAN => DataTypeInstances.Boolean
      case DECIMAL | DOUBLE | FLOAT | REAL | NUMERIC => DataTypeInstances.Fractional
      case _ => DataTypeInstances.Unknown
    }

    if (dataType != DataTypeInstances.Unknown) {
      val query =
        s"""
           |SELECT
           |  COUNT($column) AS num_not_nulls,
           |  COUNT(*) as num_rows
           |FROM
           |  ${table.name}
           |WHERE
           | ${where.getOrElse("TRUE = TRUE")};
         """.stripMargin

      val result = connection.createStatement().executeQuery(query)
      if (result.next()) {
        val num_not_nulls = result.getLong("num_not_nulls")
        val num_rows = result.getLong("num_rows")
        Some(DataTypeHistogram(
          numNull = num_rows - num_not_nulls,
          numFractional = if (dataType == DataTypeInstances.Fractional) num_not_nulls else 0,
          numIntegral = if (dataType == DataTypeInstances.Integral) num_not_nulls else 0,
          numBoolean = if (dataType == DataTypeInstances.Boolean) num_not_nulls else 0,
          numString = 0
        ))
      } else {
        None
      }
    } else {
       /*
        * Scan the column and, for each supported data type (unknown, integral, fractional,
        * boolean, string), aggregate the number of values that could be instances of the
        * respective data type. Each value is assigned to exactly one data type (i.e., data
        * types are mutually exclusive).
        * A value's data type is assumed to be unknown, if it is a NULL-value or one of the
        * following values: n.a. | null | n/a.
        * A value is integral if it contains only digits without any fractional part. If the
        * value contains only a single digit, it must not be 0 or 1, because those two values
        * are assigned to boolean.
        * A value is fractional if it contains only digits and a fractional part separated by
        * a decimal separator.
        * A value is boolean if it is equal to one of the following constants: 1 | 0 | true |
        * false | t | f | y | n | yes | no | on | off.
        */
      val integerPattern =
        """^\s*(?:-|\+)?(?:-1|[2-9]|\d{2,})\s*$"""
      val fractionPattern = """^\s*(?:-|\+)?\d+\.\d+\s*$"""
      val booleanPattern = """(?i)^\s*(?:1|0|true|false|t|f|y|n|yes|no|on|off)\s*$"""
      val nullPattern = """(?i)^\s*(?:null|N\/A|N\.?A\.?)\s*$"""
      val query =
        s"""
           |SELECT
           | COUNT($column) AS num_not_nulls,
           | COUNT(*) as num_rows,
           | SUM(CASE WHEN
           |  (SELECT regexp_matches(CAST($column AS text), '$integerPattern', '')) IS NOT NULL
           |  THEN 1 ELSE 0 END) as num_integers,
           | SUM(CASE WHEN
           |  (SELECT regexp_matches(CAST($column AS text), '$fractionPattern', '')) IS NOT NULL
           |  THEN 1 ELSE 0 END) as num_fractions,
           | SUM(CASE WHEN
           |  (SELECT regexp_matches(CAST($column AS text), '$booleanPattern', '')) IS NOT NULL
           |  THEN 1 ELSE 0 END) as num_booleans,
           | SUM(CASE WHEN
           |  (SELECT regexp_matches(CAST($column AS text), '$nullPattern', '')) IS NOT NULL
           |  THEN 1 ELSE 0 END) as num_nulls
           |FROM
           | ${table.name}
           |WHERE
           | ${where.getOrElse("TRUE = TRUE")};
      """.stripMargin

      val result = connection.createStatement().executeQuery(query)
      if (result.next()) {
        val num_not_nulls = result.getLong("num_not_nulls")
        val num_rows = result.getLong("num_rows")
        val num_integers = result.getLong("num_integers")
        val num_fractions = result.getLong("num_fractions")
        val num_booleans = result.getLong("num_booleans")
        val num_nulls = result.getLong("num_nulls")

        Some(DataTypeHistogram(
          numNull = num_nulls + (num_rows - num_not_nulls),
          numFractional = num_fractions,
          numIntegral = num_integers,
          numBoolean = num_booleans,
          numString = num_rows - (num_rows - num_not_nulls) - num_booleans - num_integers -
            num_fractions - num_nulls
        ))
      } else {
        None
      }
    }
  }

  override def computeMetricFrom(state: Option[DataTypeHistogram]): HistogramMetric = {
    state match {
      case Some(histogram) =>
        HistogramMetric(column, Success(DataTypeHistogram.toDistribution(histogram)))
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcDataType, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    HistogramMetric(column, Failure(MetricCalculationException.wrapIfNecessary(failure)))
  }
}
