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
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import Analyzers._

import com.amazon.deequ.metrics.Entity
import com.amazon.deequ.repository.AnalysisResultSerde

case class RatioOfSumsState(
    numerator: Double,
    denominator: Double
) extends DoubleValuedState[RatioOfSumsState] {

  override def sum(other: RatioOfSumsState): RatioOfSumsState = {
    RatioOfSumsState(numerator + other.numerator, denominator + other.denominator)
  }

  override def metricValue(): Double = {
    numerator / denominator
  }
}

/** Sums up 2 columns and then divides the final values as a Double. The columns
 * can contain a mix of positive and negative numbers. Dividing by zero is allowed
 * and will result in a value of Double.PositiveInfinity or Double.NegativeInfinity.
 *
 * @param numerator
 *   First input column for computation
 * @param denominator
 *   Second input column for computation
 */
case class RatioOfSums(
    numerator: String,
    denominator: String,
    where: Option[String] = None
) extends StandardScanShareableAnalyzer[RatioOfSumsState](
      "RatioOfSums",
      s"$numerator,$denominator",
      Entity.Multicolumn
    )
    with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    val firstSelection = conditionalSelection(numerator, where)
    val secondSelection = conditionalSelection(denominator, where)
    sum(firstSelection).cast(DoubleType) :: sum(secondSelection).cast(DoubleType) :: Nil
  }

  override def fromAggregationResult(
      result: Row,
      offset: Int
  ): Option[RatioOfSumsState] = {
    if (result.isNullAt(offset)) {
      None
    } else {
      Some(
        RatioOfSumsState(
          result.getDouble(0),
          result.getDouble(1)
        )
      )
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(numerator) :: isNumeric(numerator) :: hasColumn(denominator) :: isNumeric(denominator) :: Nil
  }

  override def filterCondition: Option[String] = where
}
