package com.amazon.deequ.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.Arrays;
import java.util.List;

public class JavaDataQualityExample {
    
    /**
     * Creates a DataFrame with sample data equivalent to getDfFull in Scala
     */
    public static Dataset<Row> getDfFull(SparkSession sparkSession) {
        // Define the schema
        StructType schema = new StructType(new StructField[]{
            DataTypes.createStructField("item", DataTypes.StringType, true),
            DataTypes.createStructField("att1", DataTypes.StringType, true),
            DataTypes.createStructField("att2", DataTypes.StringType, true)
        });
        
        // Create the data rows
        List<Row> data = Arrays.asList(
            org.apache.spark.sql.RowFactory.create("1", "a", "c"),
            org.apache.spark.sql.RowFactory.create("2", "a", "c"),
            org.apache.spark.sql.RowFactory.create("3", "a", "c"),
            org.apache.spark.sql.RowFactory.create("4", "b", "d")
        );
        
        // Create and return the DataFrame
        return sparkSession.createDataFrame(data, schema);
    }
    
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

            Dataset<Row> df = getDfFull(sparkSession);
            
            String ruleset = "Rules=[" +
                    "IsUnique \"item\", " +
                    "RowCount < 10, " +
                    "Completeness \"item\" > 0.8, " +
                    "Uniqueness \"item\" = 1.0]";
            
            Dataset<Row> results = com.amazon.deequ.dqdl.EvaluateDataQuality.process(df, ruleset);
            
            System.out.println("Original DataFrame:");
            df.show();
            
            System.out.println("Data Quality Results:");
            results.show();
            
        } finally {
            sparkSession.stop();
        }
    }
    
    /**
     * Alternative method using Spark's built-in row creation utilities
     */
    public static Dataset<Row> getDfFullAlternative(SparkSession sparkSession) {
        return sparkSession.sql(
            "SELECT * FROM VALUES " +
            "('1', 'a', 'c'), " +
            "('2', 'a', 'c'), " +
            "('3', 'a', 'c'), " +
            "('4', 'b', 'd') " +
            "AS t(item, att1, att2)"
        );
    }
}