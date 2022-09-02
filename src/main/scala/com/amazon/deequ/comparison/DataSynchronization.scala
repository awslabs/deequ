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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

object DataSynchronization {

  /**
   * Check if the specified dataframe columns are primary keys (all values are distinct), 
   * then join the two dataframes along the primary key columns and any other join column 
   * and compute the join ratio as a way to measure how much of the entire dataset 
   * (df1 + df2) is made up by the biggest of the two input dataframes 
   * max(df1.count(), df2.count()) / df1.count() + df2.count()
   *
   * @param df1       The first data set in which the customer will select n number
   *                  of columns to be compared.
   * @param df2       The second data set in which the customer will select n number
   *                  of columns to be compared.
   * @param primaryKeyColumns All the columns that the customer is planning to match from
   *                  the first data set.
   * @param joinColumns  The columns that will be compared 1 to 1 from both data sets,
   *                  if the customer doesn't input any column names then it will
   *                  do a full 1 to 1 check of the dataset.
   * @param assertion The assertion should check that the right ratio of dataset columns match across df1 and df2 satisfy the expected ratio
   *                  For example if df1 contains 75% of the entire dataset a passing assertion would br `_ == 0.75`
   * @return Boolean    returns false if either primary key column contains duplicates OR the user provided assertion fails, true otherwise
   */

  def columnMatch(df1: DataFrame, df2: DataFrame, primaryKeyColumns: Map[String, String],
                  joinColumns: Option[Map[String, String]],
                  assertion: Double => Boolean): Boolean = {

    val df1Unique = df1.groupBy(primaryKeyColumns.keys.toSeq.map(col): _*).count()
    val ds2Unique = df2.groupBy(primaryKeyColumns.values.toSeq.map(col): _*).count()

    if (!(df1Unique.count() == df1.count() && ds2Unique.count() == df2.count())) return false

    if (joinColumns.isDefined) {

      val mergedMaps = primaryKeyColumns.++(joinColumns.get)

      finalAssertion(df1, df2, mergedMaps, assertion)

    } else if (joinColumns.isEmpty) {

      val colsdf1 = df1.columns.filterNot(x => primaryKeyColumns.keys.toSeq.contains(x)).sorted
      val colsDS2 = df2.columns.filterNot(x => primaryKeyColumns.values.toSeq.contains(x)).sorted

      if (!(colsdf1 sameElements colsDS2)) return false

      val mergedMaps = primaryKeyColumns.++(colsdf1.map(x => x -> x).toMap)
      finalAssertion(df1, df2, mergedMaps, assertion)

    } else {
      false

    }
  }

  def finalAssertion(df1: DataFrame, df2: DataFrame, mergedMaps: Map[String, String],
                  assertion: Double => Boolean): Boolean = {

    val joinExpression: Column = mergedMaps.map { case (col1, col2) =>
      df1(col1) === df2(col2)}.reduce((e1, e2) => e1 && e2)

    val joined = df1.join(df2, joinExpression, "inner")

    val mostRows = if (df1.count() > df2.count()) df1.count() else df2.count()

    assertion(joined.count().toDouble / mostRows)

  }
}
