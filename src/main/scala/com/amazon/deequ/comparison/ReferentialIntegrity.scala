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
import org.apache.spark.sql.functions.{col, lit, when}

import scala.util.Try

object ReferentialIntegrity extends ComparisonBase {

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
   * @return ComparisonResult If validation of parameters fails, we return a "ComparisonFailed".
   *                          If validation succeeds, internally we calculate the referential integrity
   *                          as a ratio, and we run the assertion on that outcome.
   *                          That ends up being a true or false response, which translates to
   *                          ComparisonSucceeded or ComparisonFailed respectively.
   *
   */
  def subsetCheck(primary: DataFrame,
                  primaryColumns: Seq[String],
                  reference: DataFrame,
                  referenceColumns: Seq[String],
                  assertion: Double => Boolean): ComparisonResult = {
    val validatedParameters = validateParameters(primary, primaryColumns, reference, referenceColumns)

    if (validatedParameters.isDefined) {
      validatedParameters.get
    } else {
      val primaryCount = primary.count()
      val primarySparkCols = primary.select(primaryColumns.map(col): _*)
      val referenceSparkCols = reference.select(referenceColumns.map(col): _*)
      val mismatchCount = primarySparkCols.except(referenceSparkCols).count()

      val ratio = mismatchCount.toDouble / primaryCount

      if (assertion(ratio)) {
        ComparisonSucceeded()
      } else {
        ComparisonFailed(s"Value: $ratio does not meet the constraint requirement.")
      }
    }
  }

  /**
   * Annotates a given data frame with a column that contains the outcome of whether a provided set of columns
   * exist in another given data frame.
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
   * @param outcomeColumnName Name of the column that will contain the outcome results.
   *
   * @return Either[ComparisonFailed, DataFrame] If validation of parameters fails, we return a "ComparisonFailed".
   *                                             If validation succeeds, we annotate the primary data frame with a
   *                                             column that contains either true or false. That value depends on
   *                                             whether the referential integrity check succeeds for that row.
   *
   */
  def subsetCheckRowLevel(primary: DataFrame,
                          primaryColumns: Seq[String],
                          reference: DataFrame,
                          referenceColumns: Seq[String],
                          outcomeColumnName: Option[String] = None): Either[ComparisonFailed, DataFrame] = {
    val validatedParameters = validateParameters(primary, primaryColumns, reference, referenceColumns)

    if (validatedParameters.isDefined) {
      Left(validatedParameters.get)
    } else {
      // The provided columns can be nested, so we first map the column names to values that we can control
      val updatedRefColNamesMap = referenceColumns.zipWithIndex.map {
        case (col, i) => col -> s"${referenceColumnNamePrefix}_$i"
      }.toMap
      // We then add the new column names to the existing data frame
      val referenceWithUpdatedNames = updatedRefColNamesMap.foldLeft(reference) {
        case (accDf, (refColName, updatedRefColName)) => accDf.withColumn(updatedRefColName, accDf(refColName))
      }
      val updatedReferenceColNames = updatedRefColNamesMap.values.toSeq
      // We select the new column names and ensure that there are no duplicates
      val processedRef = referenceWithUpdatedNames.select(updatedReferenceColNames.map(col): _*).distinct()

      // We join on the provided list of columns from primary with the updated column names from reference
      // It will be a left join so that any rows that fail the referential integrity check will have nulls
      val joinClause = primaryColumns
        .zip(updatedReferenceColNames).map { case (colP, colR) => primary(colP) === processedRef(colR) }
        .reduce((e1, e2) => e1 && e2)

      // We cannot keep the new reference columns in the final data frame
      // Before we drop them, we need to calculate a final true/false outcome
      // If all the new columns that are added are not null, the outcome is true, otherwise it is false.
      val condition = updatedReferenceColNames.foldLeft(lit(true)) {
        case (cond, c) => cond && col(c).isNotNull
      }

      val outcomeColumn = outcomeColumnName.getOrElse(defaultOutcomeColumnName)
      Right(
        primary
          .join(processedRef, joinClause, "left")
          .withColumn(outcomeColumn, when(condition, lit(true)).otherwise(lit(false)))
          .drop(updatedReferenceColNames: _*)
      )
    }
  }

  private def validateParameters(primary: DataFrame,
                                 primaryColumns: Seq[String],
                                 reference: DataFrame,
                                 referenceColumns: Seq[String]): Option[ComparisonFailed] = {
    if (primaryColumns.isEmpty) {
      Some(ComparisonFailed(s"Empty list provided for columns to check from the primary data frame."))
    } else if (referenceColumns.isEmpty) {
      Some(ComparisonFailed(s"Empty list provided for columns to check from the reference data frame."))
    } else if (primaryColumns.size != referenceColumns.size) {
      Some(ComparisonFailed(s"The number of columns to check from the primary data frame" +
        s" must equal the number of columns to check from the reference data frame."))
    } else {
      val primaryColumnsNotInDataset = primaryColumns.filterNot(c => Try(primary(c)).isSuccess)
      val referenceColumnsNotInDataset = referenceColumns.filterNot(c => Try(reference(c)).isSuccess)

      if (primaryColumnsNotInDataset.nonEmpty) {
        primaryColumnsNotInDataset match {
          case Seq(c) => Some(ComparisonFailed(s"Column $c does not exist in primary data frame."))
          case cols => Some(ComparisonFailed(s"Columns ${cols.mkString(", ")} do not exist in primary data frame."))
        }
      } else if (referenceColumnsNotInDataset.nonEmpty) {
        referenceColumnsNotInDataset match {
          case Seq(c) => Some(ComparisonFailed(s"Column $c does not exist in reference data frame."))
          case cols => Some(ComparisonFailed(s"Columns ${cols.mkString(", ")} do not exist in reference data frame."))
        }
      } else if (primary.head(1).isEmpty) {
        Some(ComparisonFailed(s"Primary data frame contains no data."))
      } else None
    }
  }
}
