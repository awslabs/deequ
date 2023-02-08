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
   * @param primary      The primary data set which contains the column which the customer
   *                     will select the column to do the Referential Integrity check.
   * @param primaryCol   The name of the column selected from the primary data set.
   * @param reference    The reference data set which contains the possible values for the column
   *                     from the primary dataset.
   * @param referenceCol The name of the column selected from the reference data set, which
   *                     contains those values.
   * @param assertion    A function which accepts the match ratio and returns a Boolean.
   *
   * @return Boolean   Internally we calculate the referential integrity as a
   *                   ratio, and we run the assertion on that outcome
   *                   that ends up being a true or false response.
   */

  def subsetCheck(primary: DataFrame,
                  primaryCol: String,
                  reference: DataFrame,
                  referenceCol: String,
                  assertion: Double => Boolean): Boolean = {

    if (primary.columns.contains(primaryCol) && reference.columns.contains(referenceCol)) {
      val primaryCols = primary.select(primaryCol)
      val referenceCols = reference.select(referenceCol)
      val mismatchCount = primaryCols.except(referenceCols).count()

      if (mismatchCount == 0) {
        assertion(1.0)
      } else {
        val primaryCount = primary.count
        assertion((primaryCount - mismatchCount).toDouble / primaryCount)
      }
    } else {
      false
    }
  }
}
