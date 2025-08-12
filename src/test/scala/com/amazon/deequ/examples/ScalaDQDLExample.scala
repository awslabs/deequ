/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.examples

import com.amazon.deequ.dqdl.EvaluateDataQuality
import org.apache.spark.sql.SparkSession

object ScalaDQDLExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DQDL Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val df = Seq(
      ("1", "a", "c"),
      ("2", "a", "c"),
      ("3", "a", "c"),
      ("4", "b", "d")
    ).toDF("item", "att1", "att2")

    // Define rules using DQDL syntax
    val ruleset = """Rules=[IsUnique "item", RowCount < 10, Completeness "item" > 0.8, Uniqueness "item" = 1.0]"""

    // Evaluate data quality
    val results = EvaluateDataQuality.process(df, ruleset)
    results.show()
  }

}
