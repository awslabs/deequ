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

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isString}
import org.apache.spark.sql.functions.{length, min}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{Column, Row}

case class MinLength(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[MinState]("MinLength", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    min(length(conditionalSelection(column, where))).cast(DoubleType) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MinState] = {
    ifNoNullsIn(result, offset) { _ =>
      MinState(result.getDouble(offset))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isString(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}
