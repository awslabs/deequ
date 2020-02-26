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

import com.amazon.deequ.analyzers.Preconditions.hasColumn
import org.apache.spark.sql.DeequFunctions.stateful_approx_count_distinct
import org.apache.spark.sql.catalyst.expressions.aggregate.DeequHyperLogLogPlusPlusUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Row}
import Analyzers._

case class ApproxCountDistinctState(words: Array[Long])
  extends DoubleValuedState[ApproxCountDistinctState] {

  override def sum(other: ApproxCountDistinctState): ApproxCountDistinctState = {
    ApproxCountDistinctState(DeequHyperLogLogPlusPlusUtils.merge(words, other.words))
  }

  override def metricValue(): Double = {
    DeequHyperLogLogPlusPlusUtils.count(words)
  }

  override def toString: String = {
    s"ApproxCountDistinctState(${words.mkString(",")})"
  }
}

/**
  * Compute approximated count distinct with HyperLogLogPlusPlus.
  *
  * @param column Which column to compute this aggregation on.
  */
case class ApproxCountDistinct(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[ApproxCountDistinctState]("ApproxCountDistinct", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    stateful_approx_count_distinct(conditionalSelection(column, where)) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[ApproxCountDistinctState] = {

    ifNoNullsIn(result, offset) { _ =>
      DeequHyperLogLogPlusPlusUtils.wordsFromBytes(result.getAs[Array[Byte]](offset))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}
