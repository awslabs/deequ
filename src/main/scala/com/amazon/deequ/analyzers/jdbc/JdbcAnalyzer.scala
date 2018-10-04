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

package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.State
import com.amazon.deequ.metrics.Metric


trait JdbcAnalyzer[S <: State[_], +M <: Metric[_]] {

  def computeStateFrom(table: Table): Option[S]

  def computeMetricFrom(state: Option[S]): M

  def calculate(table: Table): M = {

    try {
      val state = computeStateFrom(table)
      computeMetricFrom(state)

    } catch {
      case error: Exception => toFailureMetric(error)
    }
  }

  private[deequ] def toFailureMetric(failure: Exception): M



}
