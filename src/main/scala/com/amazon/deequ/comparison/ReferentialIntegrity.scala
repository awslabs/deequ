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

import org.apache.spark.sql.{DataFrame}

object ReferentialIntegrity {

  def subsetCheck(ds1: DataFrame, col1: String, ds2: DataFrame,
                  col2: String, assertion: Double => Boolean): Boolean = {

    if (ds1.columns.contains(col1) && ds2.columns.contains(col2)) {
      val joined = ds1.join(ds2, ds1(col1) === ds2(col2), "leftsemi")
      assertion(joined.count().toDouble / ds1.count())

    } else {
      false
    }
  }
}