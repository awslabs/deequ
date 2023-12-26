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
 * To store state of DataSynchronization
 *
 * @param synchronizedDataCount - Count Of rows that are in sync
 * @param dataCount             - total count of records to caluculate ratio.
 */
case class DataSynchronizationState(synchronizedDataCount: Long, dataCount: Long)
  extends DoubleValuedState[DataSynchronizationState] {
  override def sum(other: DataSynchronizationState): DataSynchronizationState = {
    DataSynchronizationState(synchronizedDataCount + other.synchronizedDataCount, dataCount + other.dataCount)
  }

  override def metricValue(): Double = {
    if (dataCount == 0L) Double.NaN else synchronizedDataCount.toDouble / dataCount.toDouble
  }
}

object DataSynchronizationState
