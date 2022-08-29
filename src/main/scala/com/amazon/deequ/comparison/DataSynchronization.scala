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
   * Compare two DataFrames 1 to 1 with specific columns inputted by the customer.
   *
   * @param ds1       The first data set in which the customer will select n number
   *                  of columns to be compared.
   * @param ds2       The second data set in which the customer will select n number
   *                  of columns to be compared.
   * @param colKeyMap All the columns that the customer is planning to match from
   *                  the first data set.
   * @param compCols  The columns that will be compared 1 to 1 from both data sets,
   *                  if the customer doesn't input any column names then it will
   *                  do a full 1 to 1 check of the dataset.
   * @param assertion The customer inputs a function and we supply a Double to
   *                  that function, to obtain a Boolean.
   * @return Boolean    Internally we calculate the referential integrity as a
   *         percentage, and we run the assertion on that outcome
   *         that ends up being a true or false response.
   */

  def columnMatch(ds1: DataFrame, ds2: DataFrame, colKeyMap: Map[String, String],
                  compCols: Option[Map[String, String]],
                  assertion: Double => Boolean): Boolean = {

    val ds1Unique = ds1.groupBy(colKeyMap.keys.toSeq.map(col): _*).count()
    val ds2Unique = ds2.groupBy(colKeyMap.values.toSeq.map(col): _*).count()

    if (!(ds1Unique.count() == ds1.count() && ds2Unique.count() == ds2.count())) return false

    if (compCols.isDefined) {

      val mergedMaps = colKeyMap.++(compCols.get)

      finalAssertion(ds1, ds2, mergedMaps, assertion)

    } else if (compCols.isEmpty) {

      val colsDS1 = ds1.columns.filterNot(x => colKeyMap.keys.toSeq.contains(x)).sorted
      val colsDS2 = ds2.columns.filterNot(x => colKeyMap.values.toSeq.contains(x)).sorted

      if (!(colsDS1 sameElements colsDS2)) return false

      val mergedMaps = colKeyMap.++(colsDS1.map(x => x -> x).toMap)
      finalAssertion(ds1, ds2, mergedMaps, assertion)

    } else {
      false
    }
  }

  def finalAssertion(ds1: DataFrame, ds2: DataFrame, mergedMaps: Map[String, String],
                  assertion: Double => Boolean): Boolean = {
    val joinExpression: Column = mergedMaps.map { case (col1, col2) =>
      ds1(col1) === ds2(col2)
    }.reduce((e1, e2) => e1 && e2)

    val joined = ds1.join(ds2, joinExpression, "inner")

    val mostRows = if (ds1.count() > ds2.count()) ds1.count() else ds2.count()

    assertion(joined.count().toDouble / mostRows)

  }
}
