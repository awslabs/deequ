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

import org.apache.spark.sql.DataFrame

object ReferentialIntegrity {

  /** Checks to what extend a column from a DataFrame is a subset of another
    * column from another DataFrame.
    *
    * @param df1
    *   The data-frame in which the user will select the column to do the
    *   Referential Integrity check.
    * @param column1
    *   The column selected from df1.
    * @param df2
    *   The data set in which the customer chooses the second column for the
    *   Referential Integrity check.
    * @param column2
    *   The colum selected from df2.
    * @param assertion
    *   assertion to determine if the check failed / passed the referential
    *   integrity check, for example, if `df2.col2` contains 30% of the values
    *   from `df1.col1` `_ >= 0.3` would pass the check
    *
    * @return
    *   Boolean Internally we calculate the referential integrity as a
    *   percentage, and we run the assertion on that outcome that ends up being
    *   a true or false response.
    */
  def subsetCheck(
      df1: DataFrame,
      column1: String,
      df2: DataFrame,
      column2: String,
      assertion: Double => Boolean
  ): Boolean = {

    if (df1.columns.contains(column1) && df2.columns.contains(column2)) {
      val cols1 = df1.select(column1)
      val cols2 = df2.select(column2)
      val match_count = cols1.except(cols2).count()

      if (match_count == 0) {
        assertion(1.0)
      } else {
        val df1_count = df1.count
        assertion((df1_count - match_count).toDouble / df1_count)
      }
    } else {
      false
    }
  }
}
