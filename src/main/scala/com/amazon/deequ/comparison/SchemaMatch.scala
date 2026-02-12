/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 *
 */

package com.amazon.deequ.comparison

import org.apache.spark.sql.DataFrame

object SchemaMatch {
  /**
   * Compares the schemas of two DataFrames as a ratio of matching columns.
   *
   * @param primary   The primary DataFrame.
   * @param reference The reference DataFrame to compare against.
   * @param assertion A function that accepts the ratio and returns a Boolean.
   * @return ComparisonResult with the computed ratio.
   */
  def matchSchema(primary: DataFrame,
                  reference: DataFrame,
                  assertion: Double => Boolean): ComparisonResult = {
    val primarySchema = primary.schema.fields.map(f => f.name -> f.dataType.typeName).toMap
    val referenceSchema = reference.schema.fields.map(f => f.name -> f.dataType.typeName).toMap
    if (primarySchema.isEmpty || referenceSchema.isEmpty) {
      return ComparisonFailed("One or both DataFrames have no columns", 0.0)
    }
    if (primarySchema.size != referenceSchema.size) {
      return ComparisonFailed("Column counts do not match", 0.0)
    }
    val matchedColumns = primarySchema.count { case (name, typeName) =>
      referenceSchema.get(name).contains(typeName)
    }

    val ratio = matchedColumns.toDouble / primarySchema.size
    if (assertion(ratio)) {
      ComparisonSucceeded(ratio)
    } else {
      ComparisonFailed(f"${ratio * 100}%.2f%% of columns passed the threshold", ratio)
    }
  }
}
