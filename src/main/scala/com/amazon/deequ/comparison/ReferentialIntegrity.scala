/**
 * Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.comparison

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.util.Try

object ReferentialIntegrity {

  /**
   * Checks to what extent a set of columns from a DataFrame is a subset of another set of columns
   * from another DataFrame.
   *
   * This is an experimental utility.
   *
   * @param primary           The primary data set which contains the columns which the customer
   *                          will select to do the Referential Integrity check.
   * @param primaryColumns    The names of the columns selected from the primary data set.
   * @param reference         The reference data set which contains the possible values for the columns
   *                          from the primary dataset.
   * @param referenceColumns  The names of the columns selected from the reference data set, which
   *                          contains those values.
   * @param assertion         A function which accepts the match ratio and returns a Boolean.
   *
   * @return Boolean   Internally we calculate the referential integrity as a
   *                   ratio, and we run the assertion on that outcome
   *                   that ends up being a true or false response.
   */

  def subsetCheck(primary: DataFrame,
                  primaryColumns: Seq[String],
                  reference: DataFrame,
                  referenceColumns: Seq[String],
                  assertion: Double => Boolean): ComparisonResult = {
    val primaryColumnsNotInDataset = primaryColumns.filterNot(c => Try(primary(c)).isSuccess)
    val referenceColumnsNotInDataset = referenceColumns.filterNot(c => Try(reference(c)).isSuccess)

    if (primaryColumnsNotInDataset.nonEmpty) {
      primaryColumnsNotInDataset match {
        case Seq(c) => ComparisonFailed(s"Column $c does not exist in primary data frame.")
        case cols => ComparisonFailed(s"Columns ${cols.mkString(", ")} do not exist in primary data frame.")
      }
    } else if (referenceColumnsNotInDataset.nonEmpty) {
      referenceColumnsNotInDataset match {
        case Seq(c) => ComparisonFailed(s"Column $c does not exist in reference data frame.")
        case cols => ComparisonFailed(s"Columns ${cols.mkString(", ")} do not exist in reference data frame.")
      }
    } else if (primary.head(1).isEmpty) {
      ComparisonFailed(s"Primary data frame contains no data.")
    } else {
      val primaryCount = primary.count()
      val primarySparkCols = primary.select(primaryColumns.map(col): _*)
      val referenceSparkCols = reference.select(referenceColumns.map(col): _*)
      val mismatchCount = primarySparkCols.except(referenceSparkCols).count()

      val ratio = if (mismatchCount == 0) 1.0 else (primaryCount - mismatchCount).toDouble / primaryCount

      if (assertion(ratio)) {
        ComparisonSucceeded()
      } else {
        ComparisonFailed(s"Value: $ratio does not meet the constraint requirement.")
      }
    }
  }
}
