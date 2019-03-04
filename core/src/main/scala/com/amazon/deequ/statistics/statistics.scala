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

package com.amazon.deequ.statistics

import scala.util.matching.Regex

trait Statistic

case class Size(where: Option[String] = None) extends Statistic
case class Completeness(column: String, where: Option[String] = None) extends Statistic
case class Mean(column: String, where: Option[String] = None) extends Statistic
case class ApproxQuantile(column: String, quantile: Double) extends Statistic
case class Min(column: String, where: Option[String] = None) extends Statistic
case class Max(column: String, where: Option[String] = None) extends Statistic
case class StdDev(column: String, where: Option[String] = None) extends Statistic
case class Sum(column: String, where: Option[String] = None) extends Statistic
case class ApproxCountDistinct(column: String, where: Option[String] = None) extends Statistic
case class Correlation(columnA: String, columnB: String, where: Option[String] = None) extends Statistic
case class Compliance(instance: String, predicate: String, where: Option[String] = None) extends Statistic
case class PatternMatch(column: String, pattern: Regex, where: Option[String] = None) extends Statistic
case class DataType(column: String, where: Option[String] = None) extends Statistic

case class Uniqueness(columns: Seq[String]) extends Statistic
case class Distinctness(columns: Seq[String]) extends Statistic
case class UniqueValueRatio(columns: Seq[String]) extends Statistic
case class Entropy(column: String) extends Statistic
case class MutualInformation(columnA: String, columnB: String) extends Statistic


case class Histogram(
    column: String,
    maxDetailBins: Integer = Histogram.MaximumAllowedDetailBins)
  extends Statistic


object Histogram {
  val NullFieldReplacement = "NullValue"
  val MaximumAllowedDetailBins = 1000
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

private[deequ] object DataTypeInstances extends Enumeration {
  val Unknown: Value = Value(0)
  val Fractional: Value = Value(1)
  val Integral: Value = Value(2)
  val Boolean: Value = Value(3)
  val String: Value = Value(4)
}