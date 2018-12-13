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

import com.amazon.deequ.analyzers.runners.EmptyStateException
import com.amazon.deequ.metrics.DoubleMetric

class JdbcEntropy(columns: Seq[String])
  extends JdbcScanShareableFrequencyBasedAnalyzer("Entropy", columns) {

  override def calculateMetricValue(state: JdbcFrequenciesAndNumRows): DoubleMetric = {
    if (state.frequencies.isEmpty) {
      return toFailureMetric(new EmptyStateException(
        s"Empty state for analyzer JdbcEntropy, all input values were NULL."))
    }
    val numRows = state.numRows
    val entropy = state.frequencies.values.map {
      frequency => -(frequency.toDouble / numRows) * math.log(frequency.toDouble / numRows)
    }.foldLeft(0.0)(_ + _)
    toSuccessMetric(entropy)
  }
}

object JdbcEntropy {
  def apply(column: String): JdbcEntropy = {
    new JdbcEntropy(column :: Nil)
  }
}
