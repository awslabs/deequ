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

import com.amazon.deequ.analyzers.Analyzers.COUNT_COL
import com.amazon.deequ.metrics.DoubleMetric
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{col, count, lit, sum}
import org.apache.spark.sql.types.DoubleType

case class UniqueValueRatio(columns: Seq[String], where: Option[String] = None)
  extends ScanShareableFrequencyBasedAnalyzer("UniqueValueRatio", columns)
  with FilterableAnalyzer {

  override def aggregationFunctions(numRows: Long): Seq[Column] = {
    sum(col(COUNT_COL).equalTo(lit(1)).cast(DoubleType)) :: count("*") :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): DoubleMetric = {
    val numUniqueValues = result.getDouble(offset)
    val numDistinctValues = result.getLong(offset + 1).toDouble

    toSuccessMetric(numUniqueValues / numDistinctValues)
  }

  override def filterCondition: Option[String] = where
}

object UniqueValueRatio {
  def apply(column: String): UniqueValueRatio = {
    new UniqueValueRatio(column :: Nil)
  }

  def apply(column: String, where: Option[String]): UniqueValueRatio = {
    new UniqueValueRatio(column :: Nil, where)
  }
}
