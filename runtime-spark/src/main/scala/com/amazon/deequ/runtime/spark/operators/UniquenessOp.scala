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

package com.amazon.deequ.runtime.spark.operators

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.types.DoubleType

/** Uniqueness is the fraction of unique values of a column(s), i.e.,
  * values that occur exactly once. */
case class UniquenessOp(columns: Seq[String])
  extends ScanShareableFrequencyBasedOperator("Uniqueness", columns) {

  override def aggregationFunctions(numRows: Long): Seq[Column] = {
    (sum(col(Operators.COUNT_COL).equalTo(lit(1)).cast(DoubleType)) / numRows) :: Nil
  }
}

object UniquenessOp {
  def apply(column: String): UniquenessOp = {
    new UniquenessOp(column :: Nil)
  }
}
