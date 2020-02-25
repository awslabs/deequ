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

import com.amazon.deequ.metrics.Entity
import org.apache.spark.sql.{Column, Row}
import Analyzers._

case class NumMatches(numMatches: Long) extends DoubleValuedState[NumMatches] {

  override def sum(other: NumMatches): NumMatches = {
    NumMatches(numMatches + other.numMatches)
  }

  override def metricValue(): Double = {
    numMatches.toDouble
  }

}

/** Size is the number of rows in a DataFrame. */
case class Size(where: Option[String] = None)
  extends StandardScanShareableAnalyzer[NumMatches]("Size", "*", Entity.Dataset)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    conditionalCount(where) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatches] = {
    ifNoNullsIn(result, offset) { _ =>
      NumMatches(result.getLong(offset))
    }
  }

  override def filterCondition: Option[String] = where
}
