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

  def subsetCheck(
      ds1: DataFrame,
      col1: String,
      ds2: DataFrame,
      col2: String,
      assertion: Double => Boolean
  ): Boolean = {

    if (ds1.columns.contains(col1) && ds2.columns.contains(col2)) {
      val cols1 = ds1.select(col1)
      val cols2 = ds2.select(col2)
      val match_count = cols1.except(cols2).count()

      if (match_count == 0) {
        assertion(1.0)
      } else {
        val ds1_count = ds1.count
        assertion((ds1_count - match_count).toDouble / ds1_count)
      }
    } else {
      false
    }
  }
}
