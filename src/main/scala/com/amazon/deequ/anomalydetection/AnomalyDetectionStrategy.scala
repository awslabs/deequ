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

/** Interface for all strategies that spot anomalies in a series of data points. */
trait AnomalyDetectionStrategy {

  /**
    * Search for anomalies in a series of data points.
    *
    * @param dataSeries     The data contained in a Vector of Doubles
    * @param searchInterval The indices between which anomalies should be detected. [a, b).
    * @return The indices of all anomalies in the interval and their corresponding wrapper object.
    */
  def detect(
    dataSeries: Vector[Double],
    searchInterval: (Int, Int) = (0, Int.MaxValue)): Seq[(Int, Anomaly)]
}
