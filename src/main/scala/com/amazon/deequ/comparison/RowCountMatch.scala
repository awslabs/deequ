/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

object RowCountMatch {

  /**
   * Compares the row counts of two DataFrames as a ratio (primary / reference).
   *
   * @param primary   The primary DataFrame.
   * @param reference The reference DataFrame to compare against.
   * @param assertion A function that accepts the ratio and returns a Boolean.
   * @return ComparisonResult with the computed ratio.
   */
  def matchRowCounts(primary: DataFrame,
                     reference: DataFrame,
                     assertion: Double => Boolean): ComparisonResult = {
    val primaryCount = primary.count()
    val referenceCount = reference.count()
    val ratio = primaryCount.toDouble / referenceCount.toDouble

    if (assertion(ratio)) {
      ComparisonSucceeded(ratio)
    } else {
      ComparisonFailed(s"Value: $ratio does not meet the constraint requirement.", ratio)
    }
  }
}
