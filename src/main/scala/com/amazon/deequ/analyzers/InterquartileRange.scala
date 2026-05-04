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
import com.amazon.deequ.analyzers.Analyzers._
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DoubleType, StructType}

case class InterquartileRangeState(
    q1: Double,
    q3: Double)
  extends DoubleValuedState[InterquartileRangeState] {

  override def metricValue(): Double = {
    q3 - q1
  }

  // Quantiles (Q1, Q3) are not algebraically mergeable from partial
  // states, unlike mean/variance/min/max which have exact merge
  // formulas. Normal calculate() is always exact. Spark handles
  // distributed computation internally via percentile().
  //
  // TODO: Implement approximate merge using t-digest or GK sketch.
  //
  // A conservative fallback (always overestimates, never under):
  //   InterquartileRangeState(
  //     math.min(q1, other.q1),
  //     math.max(q3, other.q3))
  override def sum(other: InterquartileRangeState)
    : InterquartileRangeState = {
    throw new UnsupportedOperationException(
      "InterquartileRange states cannot be exactly merged. " +
      "Use calculate() on the full dataset instead.")
  }
}

case class InterquartileRange(
    column: String,
    where: Option[String] = None)
  extends StandardScanShareableAnalyzer[InterquartileRangeState](
    "InterquartileRange", column)
  with FilterableAnalyzer {

  private def selection: Column =
    conditionalSelection(column, where).cast(DoubleType)

  override def aggregationFunctions(): Seq[Column] = {
    expr(s"percentile($selection, 0.25)") ::
      expr(s"percentile($selection, 0.75)") :: Nil
  }

  override def fromAggregationResult(
      result: Row, offset: Int)
    : Option[InterquartileRangeState] = {
    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      InterquartileRangeState(
        result.getDouble(offset),
        result.getDouble(offset + 1))
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
