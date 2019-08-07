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

import breeze.linalg.DenseVector

/**
  * Detects anomalies based on the values' rate of change.
  * The order of the difference can be set manually.
  * If it is set to 0, this strategy acts like the [[SimpleThresholdStrategy]].
  *
  * RelativeRateOfChangeStrategy(Some(0.9), Some(1.1), 1) for example
  * calculates the first discrete difference
  * and if some point's value changes by more than 10.0 Percent in one timestep,
  * it flags it as an anomaly.
  *
  * @param maxRateDecrease Lower bound of accepted relative change (as new value / old value).
  * @param maxRateIncrease Upper bound of accepted relative change (as new value / old value).
  * @param order           Order of the calculated difference.
  *                        Set to 1 it calculates the difference between two consecutive values.
  */
case class RelativeRateOfChangeStrategy(
    maxRateDecrease: Option[Double] = None,
    maxRateIncrease: Option[Double] = None,
    order: Int = 1)
  extends BaseChangeStrategy {

  /**
    * Calculates the rate of change with respect to the specified order.
    * If the order is set to 1, the resulting value for a point at index i
    * is equal to dataSeries (i) / dataSeries(i - 1).
    * Note that this difference cannot be calculated for the first [[order]] elements in the vector.
    * The resulting vector is therefore smaller by [[order]] elements.
    *
    * @param dataSeries The values contained in a DenseVector[Double]
    * @param order      The order of the derivative.
    * @return A vector with the resulting rates of change for all values
    *         except the first [[order]] elements.
    */
  override def diff(dataSeries: DenseVector[Double], order: Int): DenseVector[Double] = {
    require(order > 0, "Order of diff cannot be zero or negative")
    if (dataSeries.length == 0) {
      dataSeries
    } else {
      val valuesRight = dataSeries.slice(order, dataSeries.length)
      val valuesLeft = dataSeries.slice(0, dataSeries.length - order)
      valuesRight / valuesLeft
    }
  }
}
