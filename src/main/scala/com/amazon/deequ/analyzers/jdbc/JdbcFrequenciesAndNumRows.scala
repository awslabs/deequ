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

case class JdbcFrequenciesAndNumRows(frequencies: Map[String, Long], numRows: Long)
  extends State[JdbcFrequenciesAndNumRows] {

  override def sum(other: JdbcFrequenciesAndNumRows): JdbcFrequenciesAndNumRows = {
    val totalRows = numRows + other.numRows
    val frequenciesSum = (frequencies.keySet ++ other.frequencies.keySet)
      .map {
        discreteValue => {
          val totalAbsolute = frequencies.getOrElse(discreteValue, 0L) +
                              other.frequencies.getOrElse(discreteValue, 0L)
          discreteValue -> totalAbsolute
        }
      }.toMap
    JdbcFrequenciesAndNumRows(frequenciesSum, totalRows)
  }
}
