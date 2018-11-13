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
import com.amazon.deequ.analyzers.CorrelationState
import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, hasTable, isNumeric}
import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.postgresql.util.PSQLException

case class JdbcCorrelation(firstColumn: String,
                           secondColumn: String,
                           where: Option[String] = None)
  extends JdbcAnalyzer[CorrelationState, DoubleMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable(firstColumn) :: hasColumn(firstColumn) :: isNumeric(firstColumn) :: hasTable(secondColumn) :: hasColumn(secondColumn) :: isNumeric(secondColumn) :: Nil
  }

  override def computeStateFrom(table: Table): Option[CorrelationState] = {

    val connection = table.jdbcConnection

    /*
    * How to calculate the Correlation:
    * calculate the mean of firstColumn (called avg_first)
    * calculate the mean of secondColumn (called avg_second)
    * subtract avg_first of every value in firstColumn (column indirectly called a)
    * subtract avg_second of every value in second Column (column indirectly called b)
    * calculate the square of the values in a (column called axa) and the square of the values in b (bxb) and the product of the values in a and b (axb)
    * sum all the values in axb (called ck, same name as in CorrelationState)
    * sum all the values in axa (called xMk, same name as in CorrelationState)
    * sum all the values in bxb (called yMk, same name as in Correlation State)
    * the result values (including the number of values (called n), avg_first (called xAvg) and avg_second (called yAvg)) are given to CorrelationState
    * Correlation State then calculates ck / math.sqrt(xMk * yMk)
    * */

    val query =
      s"""
        SELECT
          count_all AS n,
          avg_first AS xAvg,
          avg_second AS yAvg,
          SUM(axb_val) AS ck,
          SUM(axa_val) AS xMk,
          SUM(bxb_val) AS yMk
        FROM (
          SELECT
            count_all,
            avg_first,
            avg_second,
            ($firstColumn-avg_first)*($secondColumn-avg_second) AS axb_val,
            ($firstColumn-avg_first)*($firstColumn-avg_first) AS axa_val,
            ($secondColumn-avg_second)*($secondColumn-avg_second)AS bxb_val
          FROM
            (SELECT CAST($firstColumn AS NUMERIC), CAST($secondColumn AS NUMERIC)
              FROM ${table.name}
              WHERE $firstColumn IS NOT NULL AND $secondColumn IS NOT NULL AND ${where.getOrElse("TRUE=TRUE")}
              ) AS noNullValueTable,
            (SELECT
              COUNT ($firstColumn) AS count_all,
              AVG(CAST($firstColumn AS NUMERIC)) AS avg_first,
              AVG(CAST($secondColumn AS NUMERIC)) AS avg_second
            FROM
              (SELECT *
              FROM ${table.name}
              WHERE $firstColumn IS NOT NULL AND $secondColumn IS NOT NULL AND ${where.getOrElse("TRUE=TRUE")}
              ) AS noNullValueTable
            ) AS meanTable
          ) AS calculationTable
          GROUP BY count_all, avg_first, avg_second
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
      val n = result.getDouble("n")
      val xAvg = result.getDouble("xAvg")
      val yAvg = result.getDouble("yAvg")
      val ck = result.getDouble("ck")
      val xMk = result.getDouble("xMk")
      val yMk = result.getDouble("yMk")

      Some(CorrelationState(n, xAvg, yAvg, ck, xMk, yMk))
    }
    catch {
      case error: Exception => throw error
    }
  }

  override def computeMetricFrom(state: Option[CorrelationState]): DoubleMetric = {
    state match {
      case Some(theState) =>
        metricFromValue(theState.metricValue(), "Correlation", firstColumn, Entity.Column)
      case _ =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcCorrelation, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    metricFromFailure(failure, "Correlation", firstColumn, Entity.Column)
  }
}
