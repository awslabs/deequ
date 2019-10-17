/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.apache.spark.sql.{DataFrame, SparkSession}

private[examples] object ExampleUtils {
  // conf.set("spark.eventLog.enabled", "true")
  //conf.set("spark.eventLog.dir", "file:///C:/Users/me/spark/logs")
  // spark.history.fs.logDirectory
  def withSpark(func: SparkSession => Unit): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.ui.enabled", "true")
      .config("spark.eventLog.enabled","true")
      .config("spark.eventLog.dir", "/tmp/spark-events")
      .config("spark.history.fs.logDirectory ", "/tmp/spark-events")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    try {
      func(session)
    } finally {
      session.stop()
      System.clearProperty("spark.driver.port")
    }
  }

  def itemsAsDataframe(session: SparkSession, items: Item*): DataFrame = {
    val rdd = session.sparkContext.parallelize(items)
    session.createDataFrame(rdd)
  }

  def manufacturersAsDataframe(session: SparkSession, manufacturers: Manufacturer*): DataFrame = {
    val rdd = session.sparkContext.parallelize(manufacturers)
    session.createDataFrame(rdd)
  }
}
