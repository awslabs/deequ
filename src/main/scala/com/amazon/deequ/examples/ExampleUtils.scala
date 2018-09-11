package com.amazon.deequ.examples

import org.apache.spark.sql.SparkSession

private[deequ] object ExampleUtils {

  def withSpark(func: SparkSession => Unit) = {
    val session = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    try {
      func(session)
    } finally {
      session.stop()
      System.clearProperty("spark.driver.port")
    }
  }

}
