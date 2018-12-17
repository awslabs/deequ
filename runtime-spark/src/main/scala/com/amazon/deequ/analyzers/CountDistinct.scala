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

import com.amazon.deequ.metrics.DoubleMetric
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.count
import Analyzers._

case class CountDistinct(columns: Seq[String])
  extends ScanShareableFrequencyBasedAnalyzer("CountDistinct", columns) {

  override def aggregationFunctions(numRows: Long): Seq[Column] = {
    count("*") :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): DoubleMetric = {
    toSuccessMetric(result.getLong(offset).toDouble)
  }
}

object CountDistinct {
  def apply(column: String): CountDistinct = {
    new CountDistinct(column :: Nil)
  }
}
