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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, Row}

import scala.util.matching.Regex
import Operators._

/**
  * PatternMatch is a measure of the fraction of rows that complies with a given
  * column regex constraint. E.g if the constraint is Patterns.CREDITCARD and the
  * data frame has 5 rows which contain a credit card number in a certain column
  * according to the regex and and 10 rows that do not, a DoubleMetric would be
  * returned with 0.33 as value
  *
  * @param column     Column to do the pattern match analysis on
  * @param pattern    The regular expression to check for
  * @param where      Additional filter to apply before the analyzer is run.
  */
case class PatternMatchOp(column: String, pattern: Regex, where: Option[String] = None)
  extends StandardScanShareableOperator[NumMatchesAndCount]("PatternMatch", column) {

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatchesAndCount] = {
    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1))
    }
  }

  override def aggregationFunctions(): Seq[Column] = {

    val expression = when(regexp_extract(col(column), pattern.toString(), 0) =!= lit(""), 1)
      .otherwise(0)

    val summation = sum(conditionalSelection(expression, where).cast(IntegerType))

    summation :: conditionalCount(where) :: Nil
  }
}
