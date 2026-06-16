/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import org.apache.spark.sql.DeequFunctions.stateful_skewness
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types.StructType
import Analyzers._

case class SkewnessState(
    n: Double,
    avg: Double,
    m2: Double,
    m3: Double)
  extends DoubleValuedState[SkewnessState] {

  // n > 0 is guaranteed by fromAggregationResult returning None for n=0,
  // so a zero-n state is never persisted or loaded.
  require(n > 0.0, "Skewness is undefined for n = 0.")

  // Population skewness: sqrt(n) * m3 / m2^1.5
  // Returns 0 when m2 is 0 (all values identical).
  override def metricValue(): Double = {
    if (m2 == 0.0) 0.0
    else math.sqrt(n) * m3 / math.pow(m2, 1.5)
  }

  // Parallel merge of two partial states using the pairwise algorithm
  // for central moments. Same formula Spark uses in CentralMomentAgg.merge().
  // See: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
  override def sum(other: SkewnessState): SkewnessState = {
    val newN = n + other.n
    val delta = other.avg - avg
    val deltaN = if (newN == 0.0) 0.0 else delta / newN
    val deltaN2 = deltaN * deltaN

    val newM2 = m2 + other.m2 + delta * deltaN * n * other.n

    val newM3 = m3 + other.m3 +
      delta * deltaN2 * n * other.n * (n - other.n) +
      3.0 * deltaN * (n * other.m2 - other.n * m2)

    SkewnessState(newN, avg + deltaN * other.n, newM2, newM3)
  }
}

case class Skewness(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[SkewnessState](
    "Skewness", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    stateful_skewness(conditionalSelection(column, where)) :: Nil
  }

  override def fromAggregationResult(
      result: Row, offset: Int): Option[SkewnessState] = {

    if (result.isNullAt(offset)) {
      None
    } else {
      val row = result.getAs[Row](offset)
      val n = row.getDouble(0)

      if (n == 0.0) {
        None
      } else {
        Some(SkewnessState(
          n, row.getDouble(1), row.getDouble(2), row.getDouble(3)))
      }
    }
  }

  override protected def additionalPreconditions()
    : Seq[StructType => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where

  override def columnsReferenced(): Option[Set[String]] =
    if (where.isDefined) None else Some(Set(column))
}
