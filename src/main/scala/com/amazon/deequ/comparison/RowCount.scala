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

object RowCount {
  /*
This function will check the row count of the two data sets given by the customer,
and it will check the match percentage of the two data sets.

@param ds1        The first data set that the customer wants to check its row count.
@param ds2        The second data set that the customer wants to check its row count.
@param Assertion  The customer inputs a Double that will be fed to the Assertion
                  function and a boolean will be return
                  if the match percentage is within the accepted range of the customer's Assertion.

@return Boolean   Internally we calculate the referential integrity as a percentage,
                  and we run the assertion on that outcome that ends up being
                  a true or false response.
*/
  def rowCount(
                ds1: DataFrame,
                ds2: DataFrame,
                assertion: Double => Boolean): Boolean = {

    val rowsDS1 = ds1.count()
    println("po")
    val rowsDS2 = ds2.count()
    println("to")
    val rowCheck = if (rowsDS1 > rowsDS2) rowsDS2.toDouble/rowsDS1 else rowsDS1.toDouble/rowsDS2
    println("hg")
    assertion(rowCheck)
  }
}
