/** Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  * http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  */

package com.amazon.deequ.comparison

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

object DataSynchronization {

  /** @param df1:
    *   The dataset from which the keys of the mergedMap are retrieved from.
    * @param df2:
    *   The dataset from which the values of the mergedMap are retrieved from.
    * @param mergedMaps:
    *   A map that contains the columns that the user is going to compare.
    * @return
    *   Double: Returns the amount of rows of the joined DataFrame as a Double
    */
  def mergingMaps(
      df1: DataFrame,
      df2: DataFrame,
      mergedMaps: Map[String, String]
  ): Double = {
    val joinExpression: Column = mergedMaps
      .map { case (col1, col2) =>
        df1(col1) === df2(col2)
      }
      .reduce((e1, e2) => e1 && e2)

    // joining the df1 with df2 with only the rows that matches obtained from joinExpression
    val joined = df1.join(df2, joinExpression, "inner")

    joined.count().toDouble
  }

  /** Check if the specified dataframe columns are primary keys (all values are
    * distinct), then join the two dataframes along the primary key columns and
    * any other join column and compute the join ratio as a way to measure how
    * much of the entire dataset (df1 + df2) is made up by the biggest of the
    * two input dataframes max(df1.count(), df2.count()) / df1.count() +
    * df2.count()
    *
    * @param df1
    *   The first data set in which the user to be compared.
    * @param df2
    *   The second data set in which the user to be matched compared.
    * @param primaryKeyColumns
    *   DF1 Columns to be treated as primary keys (unique values) that the user
    *   is planning to match from the first.
    * @param joinColumns
    *   Columns that will be compared 1 to 1 from both data sets, if the user
    *   doesn't input any column names then it will do a full 1 to 1 check of
    *   the dataset.
    * @param assertion
    *   The assertion should check that the right ratio of dataset columns match
    *   across df1 and df2 satisfy the expected ratio For example if df1
    *   contains 75% of the entire dataset a passing assertion would br `_ ==
    *   0.75`
    * @return
    *   Boolean returns false if either primary key column contains duplicates
    *   OR the user provided assertion fails, true otherwise
    */
  def columnMatch(
      df1: DataFrame,
      df2: DataFrame,
      primaryKeyColumns: Map[String, String],
      joinColumns: Option[Map[String, String]],
      assertion: Double => Boolean
  ): Boolean = {

    val df1Unique =
      df1.groupBy(primaryKeyColumns.keys.toSeq.map(col): _*).count()
    val df2Unique =
      df2.groupBy(primaryKeyColumns.values.toSeq.map(col): _*).count()

    // Check if all the rows from the primaryKeyColumns are unique
    if (
      df1Unique.count() == df1.count() && df2Unique.count() == df2.count()
      && joinColumns.isDefined
    ) {
      // get the dataset with the most rows
      val mostRows = if (df1.count() > df2.count()) df1.count() else df2.count()

      // Merge the PrimaryKeyColumns (that we know to be unique) with the columns
      // that are going to be compared
      val mergedMaps = primaryKeyColumns.++(joinColumns.getOrElse(Map.empty))
      val count = mergingMaps(df1, df2, mergedMaps)
      assertion(count / mostRows)
    } else if (
      df1Unique.count() == df1.count() && df2Unique.count() == df2.count()
      && joinColumns.isEmpty
    ) {
      // get tye data set with the most rows
      val mostRows = if (df1.count() > df2.count()) df1.count() else df2.count()

      // collect non-primary key columns from both data sets
      val columnsDF1 =
        df1.columns
          .filterNot(x => primaryKeyColumns.keys.toSeq.contains(x))
          .sorted
      val columnsDF2 =
        df2.columns
          .filterNot(x => primaryKeyColumns.values.toSeq.contains(x))
          .sorted

      // non primary key columns must be the same
      if (!(columnsDF1 sameElements columnsDF2)) {
        return false
      }
      // Merge the primaryKeyColumns (that we know to be unique) with the
      // columns that are going to be compared
      val mergedMaps = primaryKeyColumns.++(columnsDF1.map(x => x -> x).toMap)
      val count = mergingMaps(df1, df2, mergedMaps)
      assertion(count / mostRows)
    } else {
      false
    }
  }
}
