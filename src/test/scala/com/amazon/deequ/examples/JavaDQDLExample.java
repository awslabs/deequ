/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 * <p>
 * http://aws.amazon.com/apache2.0/
 * <p>
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.deequ.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.amazon.deequ.dqdl.EvaluateDataQuality;


public class JavaDQDLExample {

    /**
     * Main method demonstrating the data quality evaluation
     */
    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("JavaDataQualityExample")
                .master("local[*]")
                .getOrCreate();

        try {

            // Create sample data
            Dataset<Row> df = sparkSession.sql(
                    "SELECT * FROM VALUES " +
                            "('1', 'a', 'c'), " +
                            "('2', 'a', 'c'), " +
                            "('3', 'a', 'c'), " +
                            "('4', 'b', 'd') " +
                            "AS t(item, att1, att2)"
            );

            // Define rules using DQDL syntax
            String ruleset = "Rules=[IsUnique \"item\", RowCount < 10, Completeness \"item\" > 0.8, Uniqueness \"item\" = 1.0]";

            // Evaluate data quality
            Dataset<Row> results = EvaluateDataQuality.process(df, ruleset);
            results.show();

        } finally {
            sparkSession.stop();
        }
    }

}
