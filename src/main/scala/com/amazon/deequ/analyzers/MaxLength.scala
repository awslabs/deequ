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
import com.amazon.deequ.analyzers.Preconditions.hasColumn
import com.amazon.deequ.analyzers.Preconditions.isString
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType

case class MaxLength(column: String, where: Option[String] = None, analyzerOptions: Option[AnalyzerOptions] = None)
  extends StandardScanShareableAnalyzer[MaxState]("MaxLength", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    max(criterion(false)) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MaxState] = {
    val convertNull: Boolean = analyzerOptions
      .map { options => options.getConvertNull() }
      .getOrElse(false)
    ifNoNullsIn(result, offset) { _ =>
      MaxState(result.getDouble(offset), Some(criterion(convertNull)))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column):: isString(column) :: Nil
  }

  override def filterCondition: Option[String] = where

  private def criterion(convertNull: Boolean): Column = {
    if (convertNull) {
      val colLengths: Column = length(conditionalSelection(column, where)).cast(IntegerType)
      conditionalSelectionForLength(colLengths, Option(s"${column} IS NULL"), Integer.MAX_VALUE)
    } else {
      length(conditionalSelection(column, where)).cast(DoubleType)
    }
  }

}
