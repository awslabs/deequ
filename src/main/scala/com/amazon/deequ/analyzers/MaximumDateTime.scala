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

import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isDateType}
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.{TimestampType, StructType}
import Analyzers._
import java.sql.Timestamp

case class MaxTimestampState(maxValue: Timestamp) extends TimestampValuedState[MaxTimestampState] {

  override def sum(other: MaxTimestampState): MaxTimestampState = {
    MaxTimestampState(if(maxValue.compareTo(other.maxValue) > 0) maxValue else other.maxValue)
  }

  override def metricValue(): Timestamp = {
    maxValue
  }
}

case class MaximumDateTime(column: String, where: Option[String] = None)
  extends TimestampScanShareableAnalyzer[MaxTimestampState]("Maximum Date Time", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    max(conditionalSelection(column, where)).cast(TimestampType) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MaxTimestampState] = {
    ifNoNullsIn(result, offset) { _ =>
        println("getting " + result.getTimestamp(offset))
        MaxTimestampState(result.getTimestamp(offset))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isDateType(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}