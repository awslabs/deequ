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

package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import com.amazon.deequ.metrics.Entity
import org.apache.spark.sql.DeequFunctions.stateful_corr
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types.StructType
import Analyzers._

case class CorrelationState(
    n: Double,
    xAvg: Double,
    yAvg: Double,
    ck: Double,
    xMk: Double,
    yMk: Double)
  extends DoubleValuedState[CorrelationState] {

  require(n > 0.0, "Correlation undefined for n = 0.")

  override def sum(other: CorrelationState): CorrelationState = {
    val n1 = n
    val n2 = other.n
    val newN = n1 + n2
    val dx = other.xAvg - xAvg
    val dxN = if (newN == 0.0) 0.0 else dx / newN
    val dy = other.yAvg - yAvg
    val dyN = if (newN == 0.0) 0.0 else dy / newN
    val newXAvg = xAvg + dxN * n2
    val newYAvg = yAvg + dyN * n2
    val newCk = ck + other.ck + dx * dyN * n1 * n2
    val newXMk = xMk + other.xMk + dx * dxN * n1 * n2
    val newYMk = yMk + other.yMk + dy * dyN * n1 * n2

    CorrelationState(newN, newXAvg, newYAvg, newCk, newXMk, newYMk)
  }

  override def metricValue(): Double = {
    ck / math.sqrt(xMk * yMk)
  }
}

/**
  * Computes the pearson correlation coefficient between the two given columns
  *
  * @param firstColumn First input column for computation
  * @param secondColumn Second input column for computation
  */
case class Correlation(
    firstColumn: String,
    secondColumn: String,
    where: Option[String] = None)
  extends StandardScanShareableAnalyzer[CorrelationState]("Correlation",
    s"$firstColumn,$secondColumn", Entity.Multicolumn)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {

    val firstSelection = conditionalSelection(firstColumn, where)
    val secondSelection = conditionalSelection(secondColumn, where)

    stateful_corr(firstSelection, secondSelection) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[CorrelationState] = {

    if (result.isNullAt(offset)) {
      None
    } else {
      val row = result.getAs[Row](offset)
      val n = row.getDouble(0)
      if (n > 0.0) {
        Some(CorrelationState(
          n,
          row.getDouble(1),
          row.getDouble(2),
          row.getDouble(3),
          row.getDouble(4),
          row.getDouble(5)))
      } else {
        None
      }
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(firstColumn) :: isNumeric(firstColumn) :: hasColumn(secondColumn) ::
      isNumeric(secondColumn) :: Nil
  }

  override def filterCondition: Option[String] = where
}
