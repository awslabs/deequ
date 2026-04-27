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
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{sum, when}
import org.apache.spark.sql.types.{IntegerType, StructType}
import Analyzers._

case class ZerosCount(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[NumMatches]("ZerosCount", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    sum(when(conditionalSelection(column, where) === 0, 1).otherwise(0)).cast(IntegerType) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatches] = {
    ifNoNullsIn(result, offset) { _ =>
      NumMatches(result.getInt(offset).toLong)
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where

  override def columnsReferenced(): Option[Set[String]] =
    if (where.isDefined) None else Some(Set(column))
}
