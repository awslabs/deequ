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
import com.amazon.deequ.analyzers.NullBehavior.NullBehavior
import com.amazon.deequ.analyzers.Preconditions.hasColumn
import com.amazon.deequ.analyzers.Preconditions.isString
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.element_at
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType

case class MaxLength(column: String, where: Option[String] = None, analyzerOptions: Option[AnalyzerOptions] = None)
  extends StandardScanShareableAnalyzer[MaxState]("MaxLength", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    // The criterion returns a column where each row contains an array of 2 elements.
    // The first element of the array is a string that indicates if the row is "in scope" or "filtered" out.
    // The second element is the value used for calculating the metric. We use "element_at" to extract it.
    max(element_at(criterion, 2).cast(DoubleType)) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MaxState] = {
    ifNoNullsIn(result, offset) { _ =>
      MaxState(result.getDouble(offset), Some(criterion))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column):: isString(column) :: Nil
  }

  override def filterCondition: Option[String] = where

  private[deequ] def criterion: Column = {
    val isNullCheck = col(column).isNull
    val colLength = length(col(column)).cast(DoubleType)
    val updatedColumn = getNullBehavior match {
      case NullBehavior.Fail => when(isNullCheck, Double.MaxValue).otherwise(colLength)
      // Empty String is 0 length string
      case NullBehavior.EmptyString => when(isNullCheck, lit(0.0)).otherwise(colLength)
      case NullBehavior.Ignore => colLength
    }

    conditionalSelectionWithAugmentedOutcome(updatedColumn, where)
  }

  private def getNullBehavior: NullBehavior = {
    analyzerOptions
      .map { options => options.nullBehavior }
      .getOrElse(NullBehavior.Ignore)
  }
}
