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

package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzers._
import com.amazon.deequ.metrics.DoubleMetric

case class JdbcUniqueValueRatio(columns: Seq[String])
  extends JdbcScanShareableFrequencyBasedAnalyzer("UniqueValueRatio", columns) {

  override def aggregationFunctions(numRows: Long): Seq[String] = {
    val noNullValue = Some(s"${columns.head} IS NOT NULL")
    val conditions = noNullValue :: Some("absolute = 1") :: Nil

    s"SUM(${conditionalSelection("1", conditions)})" ::
      conditionalCount(noNullValue) :: Nil
  }

  override def fromAggregationResult(result: JdbcRow, offset: Int): DoubleMetric = {
    val numUniqueValues = result.getDouble(offset)
    val numDistinctValues = result.getLong(offset + 1).toDouble

    toSuccessMetric(numUniqueValues / numDistinctValues)
  }

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>
        if (theState.numRows == 0 || theState.numNulls() == theState.numRows) {
          return metricFromEmpty(this, "UniqueValueRatio",
            columns.mkString(","), entityFrom(columns))
        }
      case None =>
    }
    super.computeMetricFrom(state)
  }
}

object JdbcUniqueValueRatio {
  def apply(column: String): JdbcUniqueValueRatio = {
    new JdbcUniqueValueRatio(column :: Nil)
  }
}
