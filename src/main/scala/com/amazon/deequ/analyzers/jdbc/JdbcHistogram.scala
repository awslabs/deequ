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
import com.amazon.deequ.analyzers.runners.{EmptyStateException, IllegalAnalyzerParameterException, MetricCalculationException}
import com.amazon.deequ.metrics._

import scala.util.{Failure, Try}

case class JdbcHistogram(column: String,
                         binningFunc: Option[Any => Any] = None,
                         maxDetailBins: Integer = JdbcHistogram.MaximumAllowedDetailBins)
  extends JdbcAnalyzer[JdbcFrequenciesAndNumRows, HistogramMetric] {

  private[this] val PARAM_CHECKS: Table => Unit = { _ =>
    if (maxDetailBins > JdbcHistogram.MaximumAllowedDetailBins) {
      throw new IllegalAnalyzerParameterException(s"Cannot return histogram values for more " +
        s"than ${JdbcHistogram.MaximumAllowedDetailBins} values")
    }
  }

  override def preconditions: Seq[Table => Unit] = {
    PARAM_CHECKS :: hasTable() :: hasColumn(column) :: Nil
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
        val distinctName = result.getObject("name")

        val modifiedName = binningFunc match {
          case Some(bin) => bin(distinctName)
          case _ => distinctName
        }

        val discreteValue = modifiedName match {
          case null => JdbcHistogram.NullFieldReplacement
          case _ => modifiedName.toString
        }

        val absolute = result.getLong("absolute")

        val frequency = map.getOrElse(discreteValue, 0L) + absolute
        val entry = discreteValue -> frequency
        convertResult(result, map + entry, total + absolute)
      } else {
        (map, total)
      }
    }
    val frequenciesAndNumRows = convertResult(result, Map[String, Long](), 0)
    val frequencies = frequenciesAndNumRows._1
    val numRows = frequenciesAndNumRows._2

    result.close()
    Some(JdbcFrequenciesAndNumRows(frequencies, numRows))
  }

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): HistogramMetric = {
    state match {

      case Some(theState) =>
        val value: Try[Distribution] = Try {

          val topNFreq = topNFrequencies(theState.frequencies, maxDetailBins)
          val binCount = theState.frequencies.size

          val histogramDetails = topNFreq.keys
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

  /**
    * Receive the top n key-value-pairs of a map with respect to the value.
    *
    * @param frequencies  Maps data to their occurrences.
    * @param n            The number of maximal returned frequencies.
    * @return             Biggest n key-value-pairs of frequencies with respect to the value.
    */
  def topNFrequencies(frequencies: Map[String, Long], n: Int) : Map[String, Long] = {
    if (frequencies.size <= n) {
      return frequencies
    }

    frequencies.foldLeft(Map[String, Long]()) {
      (top, i) =>
        if (top.size < n) {
          top + i
        } else if (top.minBy(_._2)._2 < i._2) {
          top - top.minBy(_._2)._1 + i
        } else {
          top
        }
    }
  }

}

object JdbcHistogram {
  val NullFieldReplacement = "NullValue"
  val MaximumAllowedDetailBins = 1000
}
