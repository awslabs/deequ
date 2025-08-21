/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.deequ.analyzers.Histogram.AggregateFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StructType}

/**
 * Base class for all histogram analyzers that provides shared functionality.
 */
abstract class HistogramBase(
                              val column: String,
                              val where: Option[String],
                              val computeFrequenciesAsRatio: Boolean,
                              val aggregateFunction: AggregateFunction)
  extends FilterableAnalyzer {

  override def filterCondition: Option[String] = where

  protected def filterOptional(where: Option[String])(data: DataFrame): DataFrame = {
    where match {
      case Some(condition) => data.filter(condition)
      case _ => data
    }
  }

  protected def query(data: DataFrame): DataFrame = {
    aggregateFunction.query(this.column, data)
  }
}
