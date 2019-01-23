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

import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzers._
import com.amazon.deequ.metrics.DoubleMetric

/**
  * Entropy is a measure of the level of information contained in a message. Given the probability
  * distribution over values in a column, it describes how many bits are required to identify a
  * value.
  */
case class JdbcEntropy(column: String)
  extends JdbcScanShareableFrequencyBasedAnalyzer("Entropy", column :: Nil) {

  override def aggregationFunctions(numRows: Long): Seq[String] = {

    val frequency = toDouble("absolute")
    val conditions = Some(s"$frequency != 0") :: Some(s"$column IS NOT NULL") :: Nil

    s"SUM(${conditionalSelection(
      s"-($frequency / $numRows) * ln($frequency / $numRows)", conditions)})" :: Nil
  }

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>
        if (theState.numNulls() == theState.numRows) {
          return metricFromEmpty(this, "Entropy",
            column, entityFrom(column :: Nil))
        }
      case None =>
    }
    super.computeMetricFrom(state)
  }
}
