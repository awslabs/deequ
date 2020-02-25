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
import org.apache.spark.sql.DeequFunctions.stateful_stddev_pop
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types.StructType
import Analyzers._

case class StandardDeviationState(
    n: Double,
    avg: Double,
    m2: Double)
  extends DoubleValuedState[StandardDeviationState] {

  require(n > 0.0, "Standard deviation is undefined for n = 0.")

  override def metricValue(): Double = {
    math.sqrt(m2 / n)
  }

  override def sum(other: StandardDeviationState): StandardDeviationState = {
    val newN = n + other.n
    val delta = other.avg - avg
    val deltaN = if (newN == 0.0) 0.0 else delta / newN

    StandardDeviationState(newN, avg + deltaN * other.n,
      m2 + other.m2 + delta * deltaN * n * other.n)
  }
}

case class StandardDeviation(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[StandardDeviationState]("StandardDeviation", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    stateful_stddev_pop(conditionalSelection(column, where)) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[StandardDeviationState] = {

    if (result.isNullAt(offset)) {
      None
    } else {
      val row = result.getAs[Row](offset)
      val n = row.getDouble(0)

      if (n == 0.0) {
        None
      } else {
        Some(StandardDeviationState(n, row.getDouble(1), row.getDouble(2)))
      }
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}
