/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * Represents the state of data synchronization between two DataFrames in Deequ.
 * This state keeps track of the count of synchronized record count and the total record count.
 * It is used to calculate a ratio of synchronization, which is a measure of how well the data
 * in the two DataFrames are synchronized.
 *
 * @param synchronizedDataCount The count of records that are considered synchronized between the two DataFrames.
 * @param totalDataCount The total count of records for check.
 *
 * The `sum` method allows for aggregation of this state with another, combining the counts from both states.
 * This is useful in distributed computations where states from different partitions need to be aggregated.
 *
 * The `metricValue` method computes the synchronization ratio. It is the ratio of `synchronizedDataCount`
 * to `dataCount`.
 * If `dataCount` is zero, which means no data points were examined, the method returns `Double.NaN`
 * to indicate the undefined state.
 *
 */
case class DataSynchronizationState(synchronizedDataCount: Long, totalDataCount: Long)
  extends DoubleValuedState[DataSynchronizationState] {
  override def sum(other: DataSynchronizationState): DataSynchronizationState = {
    DataSynchronizationState(synchronizedDataCount + other.synchronizedDataCount, totalDataCount + other.totalDataCount)
  }

  override def metricValue(): Double = {
    if (totalDataCount == 0L) Double.NaN else synchronizedDataCount.toDouble / totalDataCount.toDouble
  }
}

object DataSynchronizationState
