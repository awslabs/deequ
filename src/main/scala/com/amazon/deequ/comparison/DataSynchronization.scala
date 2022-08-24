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
   *
   * @param ds1:          The dataset from which the keys of the mergedMap are retrieved from.
   * @param ds2:          The dataset from which the values of the mergedMap are retrieved from.
   * @param mergedMaps:   A map that contains the columns that the customer is going to compare.
   * @return Double:      Returns the amount of rows of the joined DataFrame as a Double
   */

  def mergingMaps(ds1: DataFrame, ds2: DataFrame, mergedMaps: Map[String, String]): Double = {
    val joinExpression: Column = mergedMaps.map { case (col1, col2) =>
      ds1(col1) === ds2(col2)
    }.reduce((e1, e2) => e1 && e2)
    // println(joinExpression)

    // joining the ds1 with ds2 with only the rows that matches obtained from joinExpression
    val joined = ds1.join(ds2, joinExpression, "inner")
    // val joined = ds1.alias("ds1").join(ds2.alias("ds2"), joinExpression, "inner")

    // val new_dl = joined.withColumn("hash", hash(joined.columns.map(col):_*))
    // new_dl.show()


    // joined.show()
    joined.count().toDouble
  }

  def columnMatch(ds1: DataFrame, ds2: DataFrame, colKeyMap: Map[String, String],
                  compCols: Option[Map[String, String]],
                  assertion: Double => Boolean): Boolean = {

    val ds1Unique = ds1.groupBy(colKeyMap.keys.toSeq.map(col): _*).count()
    val ds2Unique = ds2.groupBy(colKeyMap.values.toSeq.map(col): _*).count()

//    val tryingMap: Column = colKeyMap.map { case (x, y) =>
//      ds1(x) === ds2(y)
//    }.reduce((e1, e2) => e1 && e2)


// hash then join again with select columns DO this

    // Check if all the rows from the colKeyMap are unique
    if (ds1Unique.count() == ds1.count() && ds2Unique.count() == ds2.count()
      && compCols.isDefined) {

      // listCompCols1.
      // println(listCompCols1)
//      val listCompCols2 = colKeyMap.ke





      // get the dataset with the most rows
      val mostRows = if (ds1.count() > ds2.count()) ds1.count() else ds2.count()

      // Merge the ColKeyMap (that we know is unique) with the columns that are going to be compared
      val mergedMaps = colKeyMap.++(compCols.get)
      val count = mergingMaps(ds1, ds2, mergedMaps)

      assertion(count / mostRows)

    } else if (ds1Unique.count() == ds1.count() && ds2Unique.count() == ds2.count()
      && compCols.isEmpty) {
      // get tye data set with the most rows
      val mostRows = if (ds1.count() > ds2.count()) ds1.count() else ds2.count()

//      collects all columns from both data sets used
      val colsDS1 = ds1.columns.filterNot(x => colKeyMap.keys.toSeq.contains(x)).sorted
      val colsDS2 = ds2.columns.filterNot(x => colKeyMap.values.toSeq.contains(x)).sorted

      if (!(colsDS1 sameElements colsDS2)) {
        return false
      }
      // val something = ds1.except(ds2)
      // println(something.show())
      //
      // Merge the ColKeyMap (that we know is unique) with the
      //      columns that are going to be compared
       val mergedMaps = colKeyMap.++(colsDS1.map(x => x -> x).toMap)
       val count = mergingMaps(ds1, ds2, mergedMaps)

      // assertion((ds1.count() - something.count()).toDouble / mostRows)
      assertion(count / mostRows)







    } else {
      false
    }
  }
}
