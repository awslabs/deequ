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

import com.amazon.deequ.metrics.Metric

/**
  * Contains utility methods to convert tuples of date and metric to a DataPoint
  */
private[deequ] object HistoryUtils {

  /**
    * Given a sequence of dated optional metrics, return sequence of dated optional metric values.
    *
    * @param metrics Sequence of dated optional metrics
    * @tparam M Type of the metric value
    * @return Sequence of dated optional metric values
    */
  def extractMetricValues[M](metrics: Seq[(Long, Option[Metric[M]])]): Seq[DataPoint[M]] = {
    metrics.map { case (date, metric) => DataPoint(date, extractMetricValue[M](metric)) }
  }

  /**
    * Given an optional metric,returns optional metric value
    *
    * @param metric Optional metric
    * @tparam M Type of the metric value
    * @return Optional metric value
    */
  def extractMetricValue[M](metric: Option[Metric[M]]): Option[M] = {
    metric.flatMap(_.value.toOption)
  }

}
