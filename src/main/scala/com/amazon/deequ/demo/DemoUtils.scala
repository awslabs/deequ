package com.amazon.deequ.demo


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

object DemoUtils {

  val NEVER: Double => Boolean = { _ == 0.0 }

  def withSpark(func: SparkSession => Unit) {
    val session = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    try {
      func(session)
    } finally {
      session.close()
      System.clearProperty("spark.driver.port")
    }
  }

  def readCSVFile(day: String, session: SparkSession): DataFrame = {
    session.read
      .option("delimiter", "\t")
      .csv(s"/home/ssc/Entwicklung/projects/deequ/data/daily/2015-08-$day/")
      .withColumnRenamed("_c0", "marketplace")
      .withColumnRenamed("_c1", "customer_id")
      .withColumnRenamed("_c2", "review_id")
      .withColumnRenamed("_c3", "product_id")
      .withColumnRenamed("_c4", "product_parent")
      .withColumnRenamed("_c5", "product_title")
      .withColumnRenamed("_c6", "product_category")
      .withColumn("star_rating", col("_c7").cast(IntegerType))
      .drop("_c7")
      .withColumnRenamed("_c7", "star_rating")
      .withColumnRenamed("_c8", "helpful_votes")
      .withColumnRenamed("_c9", "total_votes")
      .withColumnRenamed("_c10", "vine")
      .withColumnRenamed("_c11", "verified_purchase")
      .withColumnRenamed("_c12", "review_headline")
      .withColumnRenamed("_c13", "review_body")
      .withColumnRenamed("_c14", "review_date")
  }

}
