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

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isString}
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{col, lit, regexp_extract, sum, when}
import org.apache.spark.sql.types.{IntegerType, StructType}

import scala.util.matching.Regex

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
case class PatternMatch(column: String, pattern: Regex, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[NumMatchesAndCount]("PatternMatch", column)
  with FilterableAnalyzer {

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

  override def filterCondition: Option[String] = where

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isString(column) :: Nil
  }
}

object Patterns {

  // scalastyle:off
  // http://emailregex.com
  val EMAIL: Regex = """(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])""".r

  // https://mathiasbynens.be/demo/url-regex stephenhay
  val URL: Regex = """(https?|ftp)://[^\s/$.?#].[^\s]*""".r

  val SOCIAL_SECURITY_NUMBER_US: Regex = """((?!219-09-9999|078-05-1120)(?!666|000|9\d{2})\d{3}-(?!00)\d{2}-(?!0{4})\d{4})|((?!219 09 9999|078 05 1120)(?!666|000|9\d{2})\d{3} (?!00)\d{2} (?!0{4})\d{4})|((?!219099999|078051120)(?!666|000|9\d{2})\d{3}(?!00)\d{2}(?!0{4})\d{4})""".r

  // Visa, MasterCard, AMEX, Diners Club
  // http://www.richardsramblings.com/regex/credit-card-numbers/
  val CREDITCARD: Regex = """\b(?:3[47]\d{2}([\ \-]?)\d{6}\1\d|(?:(?:4\d|5[1-5]|65)\d{2}|6011)([\ \-]?)\d{4}\2\d{4}\2)\d{4}\b""".r
  // scalastyle:on
}
