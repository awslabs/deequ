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

import java.sql.ResultSet

import com.amazon.deequ.analyzers.StandardDeviationState
import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzers._
import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasColumn, hasNoInjection, isNumeric}

case class JdbcStandardDeviation(column: String, where: Option[String] = None)
  extends JdbcStandardScanShareableAnalyzer[StandardDeviationState](
    "StandardDeviation", column) {

  override def aggregationFunctions() : Seq[String] = {
    conditionalCountNotNull(column, where) :: s"SUM(${conditionalSelection(column, where)})" ::
      s"SUM(POWER(${conditionalSelection(column, where)}, 2))" :: Nil
  }

  override def fromAggregationResult(result: JdbcRow, offset: Int)
    : Option[StandardDeviationState] = {
      ifNoNullsIn(result, offset, 3) { _ =>
        val num_rows = result.getDouble(offset)
        val col_sum = result.getDouble(offset + 1)
        val col_sum_squared = result.getDouble(offset + 2)
        val col_avg : Double = col_sum / num_rows
        val m2 : Double = col_sum_squared - col_sum * col_sum / num_rows
        StandardDeviationState(num_rows, col_avg, m2)
    }
  }

  override def additionalPreconditions(): Seq[Table => Unit] = {
    hasColumn(column) :: isNumeric(column) :: hasNoInjection(where) :: Nil
  }
}
