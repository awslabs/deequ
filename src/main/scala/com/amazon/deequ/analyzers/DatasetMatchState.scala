/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.analyzers

/**
 * Represents the state of datasetMatch between two DataFrames in Deequ.
 * This state keeps track of the count of matched record count and the total record count.
 * It measures how well the data in the two DataFrames matches.
 *
 * @param matchedDataCount The count of records that are considered match between the two DataFrames.
 * @param totalDataCount The total count of records for check.
 *
 * The `sum` method allows for aggregation of this state with another, combining the counts from both states.
 * This is useful in distributed computations where states from different partitions need to be aggregated.
 *
 * The `metricValue` method computes the synchronization ratio. It is the ratio of `matchedDataCount` to `dataCount`.
 * If `dataCount` is zero, which means no data points were examined, the method returns `Double.NaN` to indicate
 * the undefined state.
 *
 */
case class DatasetMatchState(matchedDataCount: Long, totalDataCount: Long)
  extends DoubleValuedState[DatasetMatchState] {
  override def sum(other: DatasetMatchState): DatasetMatchState = {
    DatasetMatchState(matchedDataCount + other.matchedDataCount, totalDataCount + other.totalDataCount)
  }

  override def metricValue(): Double = {
    if (totalDataCount == 0L) Double.NaN else matchedDataCount.toDouble / totalDataCount.toDouble
  }
}

object DatasetMatchState
