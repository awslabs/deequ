/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.deequ.analyzers.Analyzers.metricFromFailure
import com.amazon.deequ.comparison.DataSynchronization
import com.amazon.deequ.comparison.DataSynchronizationFailed
import com.amazon.deequ.comparison.DataSynchronizationSucceeded
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import org.apache.spark.sql.DataFrame

import scala.util.Failure
import scala.util.Try


/**
 * An Analyzer for Deequ that performs a data synchronization check between two DataFrames.
 * It evaluates the degree of synchronization based on specified column mappings and an assertion function.
 *
 * The analyzer computes a ratio of synchronized data points to the total data points, represented as a DoubleMetric.
 * Refer to [[com.amazon.deequ.comparison.DataSynchronization.columnMatch]] for DataSynchronization implementation
 *
 * @param dfToCompare The DataFrame to compare with the primary DataFrame that is setup
 *                    during [[com.amazon.deequ.VerificationSuite.onData]] setup.
 * @param columnMappings A map where each key-value pair represents a column in the primary DataFrame
 *                       and its corresponding column in dfToCompare.
 * @param assertion A function that takes a Double (the match ratio) and returns a Boolean.
 *                  It defines the condition for successful synchronization.
 *
 * Usage:
 * This analyzer is used in Deequ's VerificationSuite based if `isDataSynchronized` check is defined or could be used
 * manually as well.
 *
 * Example:
 * val analyzer = DataSynchronizationAnalyzer(dfToCompare, Map("col1" -> "col2"), _ > 0.8)
 * val verificationResult = VerificationSuite().onData(df).addAnalyzer(analyzer).run()
 *
 * // or could do something like below
 * val verificationResult = VerificationSuite().onData(df).isDataSynchronized(dfToCompare, Map("col1" -> "col2"),
 *                                                                              _ > 0.8).run()
 *
 *
 * The computeStateFrom method calculates the synchronization state by comparing the specified columns of the two
 * DataFrames.
 * The computeMetricFrom method then converts this state into a DoubleMetric representing the synchronization ratio.
 *
 */
case class DataSynchronizationAnalyzer(dfToCompare: DataFrame,
                                       columnMappings: Map[String, String],
                                       assertion: Double => Boolean)
  extends Analyzer[DataSynchronizationState, DoubleMetric] {

  override def computeStateFrom(data: DataFrame): Option[DataSynchronizationState] = {

    val result = DataSynchronization.columnMatch(data, dfToCompare, columnMappings, assertion)

    result match {
      case succeeded: DataSynchronizationSucceeded =>
        Some(DataSynchronizationState(succeeded.passedCount, succeeded.totalCount))
      case failed: DataSynchronizationFailed =>
        Some(DataSynchronizationState(failed.passedCount.getOrElse(0), failed.totalCount.getOrElse(0)))
      case _ => None
    }
  }

  override def computeMetricFrom(state: Option[DataSynchronizationState]): DoubleMetric = {

    val metric = state match {
      case Some(s) => Try(s.synchronizedDataCount.toDouble / s.totalDataCount.toDouble)
      case _ => Failure(new IllegalStateException("No state available for DataSynchronizationAnalyzer"))
    }

    DoubleMetric(Entity.Dataset, "DataSynchronization", "", metric, None)
  }

  override private[deequ] def toFailureMetric(failure: Exception) =
    metricFromFailure(failure, "DataSynchronization", "", Entity.Dataset)
}

