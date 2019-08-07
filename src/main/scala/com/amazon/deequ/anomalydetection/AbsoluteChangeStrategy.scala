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

package com.amazon.deequ.anomalydetection

/**
  * Detects anomalies based on the values' absolute change.
  * The order of the difference can be set manually.
  * If it is set to 0, this strategy acts like the [[SimpleThresholdStrategy]].
  *
  * AbsoluteChangeStrategy(Some(-10.0), Some(10.0), 1) for example
  * calculates the first discrete difference
  * and if some point's value changes by more than 10.0 in one timestep, it flags it as an anomaly.
  *
  * @param maxRateDecrease Upper bound of accepted decrease (lower bound of increase).
  * @param maxRateIncrease Upper bound of accepted growth.
  * @param order           Order of the calculated difference.
  *                        Set to 1 it calculates the difference between two consecutive values.
  */
case class AbsoluteChangeStrategy(
  maxRateDecrease: Option[Double] = None,
  maxRateIncrease: Option[Double] = None,
  order: Int = 1) extends BaseChangeStrategy
