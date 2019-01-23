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

import com.amazon.deequ.analyzers.NumMatches
import com.amazon.deequ.analyzers.jdbc.Preconditions.{hasNoInjection, hasTable}
import com.amazon.deequ.metrics.Entity

case class JdbcSize(where: Option[String] = None)
  extends JdbcStandardScanShareableAnalyzer[NumMatches]("Size", "*", Entity.Dataset) {

  override def preconditions: Seq[Table => Unit] = {
    hasTable() :: hasNoInjection(where):: Nil
  }

  override def aggregationFunctions(): Seq[String] = {
    JdbcAnalyzers.conditionalCount(where) :: Nil
  }

  override def fromAggregationResult(result: JdbcRow, offset: Int): Option[NumMatches] = {
    JdbcAnalyzers.ifNoNullsIn(result, offset) { _ =>
      NumMatches(result.getLong(offset))
    }
  }
}
