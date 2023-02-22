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

/**
 * Compare two DataFrames 1 to 1 with specific columns inputted by the customer.
 *
 * This is an experimental utility.
 *
 * For example, consider the two dataframes below:
 *
 * DataFrame A:
 *
 * |--ID--|---City---|--State--|
 * |  1   | New York |    NY   |
 * |  2   | Chicago  |    IL   |
 * |  3   | Boston   |    MA   |
 *
 * DataFrame B:
 *
 * |--CityID--|---City---|-----State-----|
 * |     1    | New York |    New York   |
 * |     2    | Chicago  |    Illinois   |
 * |     3    | Boston   | Massachusetts |
 *
 * Note that dataframe B is almost equal to dataframe B, but for two things:
 *  1) The ID column in B is called CityID
 *  2) The State column in B is the full name, whereas A uses the abbreviation.
 *
 * To compare A with B, for just the City column, we can use the function like the following.
 *
 * DataSynchronization.columnMatch(
 *   ds1 = dfA,
 *   ds2 = dfB,
 *   colKeyMap = Map("ID" -> "CityID"), // Mapping for the key columns
 *   compCols = Map("City" -> "City"), // Mapping for the columns that should be compared
 *   assertion = _ > 0.8
 * )
 *
 * This will evaluate to true since the City column matches in A and B for the corresponding ID.
 *
 * To compare A with B, for all columns, we can use the function like the following.
 *
 * DataSynchronization.columnMatch(
 *   ds1 = dfA,
 *   ds2 = dfB,
 *   colKeyMap = Map("ID" -> "CityID"), // Mapping for the key columns
 *   assertion = _ > 0.8
 * )
 *
 * This will evaluate to false. The city column will match, but the state column will not.
 */

object DataSynchronization {
  /**
   * This will evaluate to false. The city column will match, but the state column will not.
   *
   * @param ds1                The first data set which the customer will select for comparison.
   * @param ds2                The second data set which the customer will select for comparison.
   * @param colKeyMap          A map of columns to columns used for joining the two datasets.
   *                           The keys in the map are composite key forming columns from the first dataset.
   *                           The values for each key is the equivalent column from the second dataset.
   * @param assertion          A function which accepts the match ratio and returns a Boolean.
   * @return ComparisonResult  An appropriate subtype of ComparisonResult is returned.
   *                           Once all preconditions are met, we calculate the ratio of the rows
   *                           that match and we run the assertion on that outcome.
   *                           The response is then converted to ComparisonResult.
   */
  def columnMatch(ds1: DataFrame,
                  ds2: DataFrame,
                  colKeyMap: Map[String, String],
                  assertion: Double => Boolean): ComparisonResult = {
    if (areKeyColumnsValid(ds1, ds2, colKeyMap)) {
      val colsDS1 = ds1.columns.filterNot(x => colKeyMap.keys.toSeq.contains(x)).sorted
      val colsDS2 = ds2.columns.filterNot(x => colKeyMap.values.toSeq.contains(x)).sorted

      if (!(colsDS1 sameElements colsDS2)) {
        ComparisonFailed("Non key columns in the given data frames do not match.")
      } else {
        val mergedMaps = colKeyMap ++ colsDS1.map(x => x -> x).toMap
        finalAssertion(ds1, ds2, mergedMaps, assertion)
      }
    } else {
      ComparisonFailed("Provided key map not suitable for given data frames.")
    }
  }

  /**
   * This will evaluate to false. The city column will match, but the state column will not.
   *
   * @param ds1                The first data set which the customer will select for comparison.
   * @param ds2                The second data set which the customer will select for comparison.
   * @param colKeyMap          A map of columns to columns used for joining the two datasets.
   *                           The keys in the map are composite key forming columns from the first dataset.
   *                           The values for each key is the equivalent column from the second dataset.
   * @param compCols           A map of columns to columns which we will check for equality, post joining.
   * @param assertion          A function which accepts the match ratio and returns a Boolean.
   * @return ComparisonResult  An appropriate subtype of ComparisonResult is returned.
   *                           Once all preconditions are met, we calculate the ratio of the rows
   *                           that match and we run the assertion on that outcome.
   *                           The response is then converted to ComparisonResult.
   */
  def columnMatch(ds1: DataFrame,
                  ds2: DataFrame,
                  colKeyMap: Map[String, String],
                  compCols: Map[String, String],
                  assertion: Double => Boolean): ComparisonResult = {
    if (areKeyColumnsValid(ds1, ds2, colKeyMap)) {
      val mergedMaps = colKeyMap ++ compCols
      finalAssertion(ds1, ds2, mergedMaps, assertion)
    } else {
      ComparisonFailed("Provided key map not suitable for given data frames.")
    }
  }

  private def areKeyColumnsValid(ds1: DataFrame,
                                 ds2: DataFrame,
                                 colKeyMap: Map[String, String]): Boolean = {
    // We verify that the key columns provided form a valid primary/composite key.
    // To achieve this, we group the dataframes and compare their count with the original count.
    // If the key columns provided are valid, then the two columns should match.
    val ds1Unique = ds1.groupBy(colKeyMap.keys.toSeq.map(col): _*).count()
    val ds2Unique = ds2.groupBy(colKeyMap.values.toSeq.map(col): _*).count()
    (ds1Unique.count() == ds1.count()) && (ds2Unique.count() == ds2.count())
  }

  private def finalAssertion(ds1: DataFrame,
                             ds2: DataFrame,
                             mergedMaps: Map[String, String],
                             assertion: Double => Boolean): ComparisonResult = {

    val ds1Count = ds1.count()
    val ds2Count = ds2.count()

    if (ds1Count != ds2Count) {
      ComparisonFailed(s"The row counts of the two data frames do not match.")
    } else {
      val joinExpression: Column = mergedMaps
        .map { case (col1, col2) => ds1(col1) === ds2(col2)}
        .reduce((e1, e2) => e1 && e2)

      val joined = ds1.join(ds2, joinExpression, "inner")
      val ratio = joined.count().toDouble / ds1Count

      if (assertion(ratio)) {
        ComparisonSucceeded()
      } else {
        ComparisonFailed(s"Value: $ratio does not meet the constraint requirement.")
      }
    }
  }
}
