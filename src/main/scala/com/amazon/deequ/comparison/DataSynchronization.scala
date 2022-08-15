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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


object DataSynchronization {

  def columnMatch(ds1: DataFrame, ds2: DataFrame, colKeyMap: Map[String, String],
                  compCols: Option[Map[String, String]], assertion: Double => Boolean): Boolean = {

    val ds1_count = ds1.groupBy(colKeyMap.keys.toSeq.map(col):_*).count()
    val ds2_count = ds2.groupBy(colKeyMap.values.toSeq.map(col):_*).count()
    //val columns = Map[ds1 -> ds2]

    //Check if all the rows from the colKeyMap are unique
    if(ds1_count.count() == ds1.count() && ds2_count.count() == ds2.count() && !compCols.isEmpty) {
        //get the dataset with the most amount of rows
        val mostRows = if (ds1.count() > ds2.count()) ds1.count() else ds2.count()

        println(ds1.printSchema())
        ds1.select(column(""))

        //Merge the ColKeyMap (that we know is unique) with the columns that are going to be compared
        val mergedMaps = colKeyMap.++(compCols)

        // mapping the merged map creating a "case" (string, string) only when the name matches
        // then it changes the matching pairs for e1 && e2 for the join expression
        val joinExpression: Column = mergedMaps.map { case (col1, col2) =>
          ds1(col1) === ds2(col2)
        }.reduce((e1, e2) => e1 && e2)

        //joining the ds1 with ds2 with only the rows that matches obtained from joinExpression
        val joined = ds1.join(ds2, joinExpression, "leftsemi")

        assertion(joined.count().toDouble / mostRows)
    }else if(ds1_count.count() == ds1.count() && ds2_count.count() == ds2.count() && compCols.isEmpty){
        val mostRows = if (ds1.count() > ds2.count()) ds1.count() else ds2.count()

        //collect all of the columns from ds1 and ds2 and map them


        //Merge the ColKeyMap (that we know is unique) with the columns that are going to be compared
        val mergedMaps = colKeyMap.++(compCols)

        // mapping the merged map creating a "case" (string, string) only when the name matches
        // then it changes the matching pairs for e1 && e2 for the join expression
        val joinExpression: Column = mergedMaps.map { case (col1, col2) =>
          ds1(col1) === ds2(col2)
        }.reduce((e1, e2) => e1 && e2)

        //joining the ds1 with ds2 with only the rows that matches obtained from joinExpression
        val joined = ds1.join(ds2, joinExpression, "leftsemi")

        assertion(joined.count().toDouble / mostRows)
      } else{
        false
    }
  }
}