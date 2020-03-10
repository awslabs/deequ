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

package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Analyzers.COUNT_COL
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, sum, udf}

/**
  * Entropy is a measure of the level of information contained in a message. Given the probability
  * distribution over values in a column, it describes how many bits are required to identify a
  * value.
  */
case class Entropy(column: String, where: Option[String] = None)
  extends ScanShareableFrequencyBasedAnalyzer("Entropy", column :: Nil)
  with FilterableAnalyzer {

  override def aggregationFunctions(numRows: Long): Seq[Column] = {
    val summands = udf { (count: Double) =>
      if (count == 0.0) {
        0.0
      } else {
        -(count / numRows) * math.log(count / numRows)
      }
    }

    sum(summands(col(COUNT_COL))) :: Nil
  }

  override def filterCondition: Option[String] = where
}
