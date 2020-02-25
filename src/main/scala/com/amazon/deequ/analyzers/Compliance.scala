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

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions._
import Analyzers._

/**
  * Compliance is a measure of the fraction of rows that complies with the given column constraint.
  * E.g if the constraint is "att1>3" and data frame has 5 rows with att1 column value greater than
  * 3 and 10 rows under 3; a DoubleMetric would be returned with 0.33 value
  *
  * @param instance         Unlike other column analyzers (e.g completeness) this analyzer can not
  *                         infer to the metric instance name from column name.
  *                         Also the constraint given here can be referring to multiple columns,
  *                         so metric instance name should be provided,
  *                         describing what the analysis being done for.
  * @param predicate SQL-predicate to apply per row
  * @param where Additional filter to apply before the analyzer is run.
  */
case class Compliance(instance: String, predicate: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[NumMatchesAndCount]("Compliance", instance)
  with FilterableAnalyzer {

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatchesAndCount] = {

    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1))
    }
  }

  override def aggregationFunctions(): Seq[Column] = {

    val summation = sum(conditionalSelection(expr(predicate), where).cast(IntegerType))

    summation :: conditionalCount(where) :: Nil
  }

  override def filterCondition: Option[String] = where
}
