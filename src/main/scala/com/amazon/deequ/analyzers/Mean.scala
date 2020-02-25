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
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.types.{DoubleType, StructType, LongType}
import Analyzers._

case class MeanState(sum: Double, count: Long) extends DoubleValuedState[MeanState] {

  override def sum(other: MeanState): MeanState = {
    MeanState(sum + other.sum, count + other.count)
  }

  override def metricValue(): Double = {
    if (count == 0L) Double.NaN else sum / count
  }
}

case class Mean(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[MeanState]("Mean", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    sum(conditionalSelection(column, where)).cast(DoubleType) ::
      count(conditionalSelection(column, where)).cast(LongType) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MeanState] = {

    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      MeanState(result.getDouble(offset), result.getLong(offset + 1))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}
