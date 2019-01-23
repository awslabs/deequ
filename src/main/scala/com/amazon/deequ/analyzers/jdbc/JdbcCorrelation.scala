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

import com.amazon.deequ.analyzers.CorrelationState
import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzers._
import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, hasNoInjection, isNumeric}
import com.amazon.deequ.metrics.Entity

/**
  * Computes the pearson correlation coefficient between the two given columns
  *
  * @param firstColumn First input column for computation
  * @param secondColumn Second input column for computation
  */
case class JdbcCorrelation(
      firstColumn: String,
      secondColumn: String,
      where: Option[String] = None)
  extends JdbcStandardScanShareableAnalyzer[CorrelationState]("Correlation",
    s"$firstColumn,$secondColumn", Entity.Mutlicolumn) {

  override def aggregationFunctions(): Seq[String] = {

    s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
      s"$firstColumn * $secondColumn")})" ::
    s"AVG(${conditionalNotNull(firstColumn, secondColumn, where,
      s"$firstColumn")})" ::
    s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
      s"$secondColumn")})" ::
    s"AVG(${conditionalNotNull(firstColumn, secondColumn, where,
      s"$secondColumn")})" ::
    s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
      s"$firstColumn")})" ::
    s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
      s"1")})" ::
    s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
      s"$firstColumn * $firstColumn")})" ::
    s"SUM(${conditionalNotNull(firstColumn, secondColumn, where,
      s"$secondColumn * $secondColumn")})" ::
    Nil

  }

  override def fromAggregationResult(result: JdbcRow, offset: Int): Option[CorrelationState] = {
    ifNoNullsIn(result, offset, 8) { _ =>
      val numRows = result.getDouble(5)
      val sumFirstTimesSecond = result.getDouble(0)
      val averageFirst = result.getDouble(1)
      val sumSecond = result.getDouble(2)
      val averageSecond = result.getDouble(3)
      val sumFirst = result.getDouble(4)
      val sumFirstSquared = result.getDouble(6)
      val sumSecondSquared = result.getDouble(7)

      val ck = sumFirstTimesSecond - (averageSecond * sumFirst) - (averageFirst * sumSecond) +
        (numRows * averageFirst * averageSecond)
      val xmk = sumFirstSquared - (averageFirst * sumFirst) - (averageFirst * sumFirst) +
        (numRows * averageFirst * averageFirst)
      val ymk = sumSecondSquared - 2 * averageSecond * sumSecond +
        (numRows * averageSecond * averageSecond)

      CorrelationState(
        numRows,
        averageFirst,
        averageSecond,
        ck,
        xmk,
        ymk)
    }
  }

  override protected def additionalPreconditions(): Seq[Table => Unit] = {
    hasColumn(firstColumn) :: isNumeric(firstColumn) :: hasColumn(secondColumn) ::
      isNumeric(secondColumn) :: hasNoInjection(where) :: Nil
  }
}
