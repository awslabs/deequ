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
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Column

/**
  * Distinctness is the fraction of distinct values of a column(s).
  *
  * @param columns  the column(s) for which to compute distinctness
  */
case class Distinctness(columns: Seq[String], where: Option[String] = None)
  extends ScanShareableFrequencyBasedAnalyzer("Distinctness", columns)
  with FilterableAnalyzer {

  override def aggregationFunctions(numRows: Long): Seq[Column] = {
    (sum(col(COUNT_COL).geq(1).cast(DoubleType)) / numRows) :: Nil
  }

  override def filterCondition: Option[String] = where
}

object Distinctness {
  def apply(column: String): Distinctness = {
    new Distinctness(column :: Nil)
  }
}
