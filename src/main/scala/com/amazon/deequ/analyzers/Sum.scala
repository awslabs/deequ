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
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{Column, Row}
import Analyzers._

case class SumState(sum: Double) extends DoubleValuedState[SumState] {

  override def sum(other: SumState): SumState = {
    SumState(sum + other.sum)
  }

  override def metricValue(): Double = {
    sum
  }
}

case class Sum(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[SumState]("Sum", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    sum(conditionalSelection(column, where)).cast(DoubleType) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[SumState] = {
    ifNoNullsIn(result, offset) { _ =>
      SumState(result.getDouble(offset))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}
