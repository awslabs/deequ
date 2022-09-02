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

object ReferentialIntegrity {

  /**
   * Checks to what extend a column from a DataFrame is a subset of another column
   * from another DataFrame.
   *
   * @param df1        The data-frame in which the user will select the column to
   *                   do the Referential Integrity check.
   * @param col1       The column selected from df1.
   * @param df2        The data set in which the customer chooses the second column
   *                   for the Referential Integrity check.
   * @param col2       The colum selected from df2.
   * @param assertion  assertion to determine if the check failed / passed the referential integrity check, for example, if `df2.col2` contains 30% of the values from `df1.col1` `_ >= 0.3` would pass the check
   *
   * @return Boolean   Internally we calculate the referential integrity as a
   *                   percentage, and we run the assertion on that outcome
   *                   that ends up being a true or false response.
   */

  def subsetCheck(df1: DataFrame, col1: String, df2: DataFrame,
                  col2: String, assertion: Double => Boolean): Boolean = {

    if (df1.columns.contains(col1) && df2.columns.contains(col2)) {

      val cols1 = df1.select(col1)
      val cols2 = df2.select(col2)
      val mismatch_count = cols1.except(cols2).count()

      if (mismatch_count == 0) {
        assertion(1.0)

      } else {
        val ds1_count = df1.count
        assertion((ds1_count - mismatch_count).toDouble / ds1_count)

      }
    } else {
      false

    }
  }
}
