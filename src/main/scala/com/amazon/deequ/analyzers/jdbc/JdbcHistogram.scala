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

import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, hasTable}
import com.amazon.deequ.analyzers.runners.{EmptyStateException, MetricCalculationException}
import com.amazon.deequ.metrics._

import scala.util.{Failure, Try}

case class JdbcHistogram(column: String)
  extends JdbcAnalyzer[JdbcFrequenciesAndNumRows, HistogramMetric] {

  override def preconditions: Seq[Table => Unit] = {
    hasTable() :: hasColumn(column) :: Nil
  }

  override def computeStateFrom(table: Table): Option[JdbcFrequenciesAndNumRows] = {

    val connection = table.jdbcConnection

    val query =
      s"""
         | SELECT $column as name, COUNT(*) AS absolute
         |    FROM ${table.name}
         |    GROUP BY $column
        """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    def convertResult(resultSet: ResultSet,
                      map: Map[String, Long],
                      total: Long): (Map[String, Long], Long) = {
      if (result.next()) {
        val columnName = result.getObject("name")
        val discreteValue = columnName match {
          case null => "NullValue"
          case _ => columnName.toString
        }
        val absolute = result.getLong("absolute")
        val entry = discreteValue -> absolute
        convertResult(result, map + entry, total + absolute)
      } else {
        (map, total)
      }
    }
    val frequenciesAndNumRows = convertResult(result, Map[String, Long](), 0)
    val frequencies = frequenciesAndNumRows._1
    val numRows = frequenciesAndNumRows._2

    Some(JdbcFrequenciesAndNumRows(frequencies, numRows))
    
    val frequenciesAndNumRows = convertResult(result, Map[String, DistributionValue](), 0)
    val frequencies = frequenciesAndNumRows._1

    val distribution = Distribution(frequencies, frequencies.size)
    Some(JdbcFrequenciesAndNumRows(distribution, frequenciesAndNumRows._2))
  }

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): HistogramMetric = {
    state match {

      case Some(theState) =>
        val value: Try[Distribution] = Try {
          // TODO: think about sorting

          // val topNRows = theState.frequencies.rdd.top(maxDetailBins)(OrderByAbsoluteCount)
          val binCount = theState.frequencies.size

          val histogramDetails = theState.frequencies.keys
            .map { discreteValue: String =>
              val absolute = theState.frequencies(discreteValue)
              val ratio = absolute.toDouble / theState.numRows
              discreteValue -> DistributionValue(absolute, ratio)
            }
            .toMap

          Distribution(histogramDetails, binCount)
        }

        HistogramMetric(column, value)

      case None =>
        toFailureMetric(new EmptyStateException(
          s"Empty state for analyzer JdbcCompleteness, all input values were NULL."))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception): HistogramMetric = {
    HistogramMetric(column, Failure(MetricCalculationException.wrapIfNecessary(failure)))
  }
}
