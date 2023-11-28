/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

import java.nio.file.{Files, Path}
import scala.collection.convert.ImplicitConversions.`iterator asScala`

/**
  * To be mixed with Tests so they can use a default spark context suitable for testing
  */
trait SparkContextSpec {

  val tmpWareHouseDir: Path = Files.createTempDirectory("deequ_tmp")

  /**
    * @param testFun thunk to run with SparkSession as an argument
    */
  def withSparkSession(testFun: SparkSession => Any): Unit = {
    val session = setupSparkSession()
    try {
      testFun(session)
    } finally {
      /* empty cache of RDD size, as the referred ids are only valid within a session */
      tearDownSparkSession(session)
    }
  }

  def withSparkSessionCustomWareHouse(testFun: SparkSession => Any): Unit = {
    val session = setupSparkSession(Some(tmpWareHouseDir.toAbsolutePath.toString))
    try {
      testFun(session)
    } finally {
      tearDownSparkSession(session)
    }
  }

  def withSparkSessionIcebergCatalog(testFun: SparkSession => Any): Unit = {
    val session = setupSparkSession(Some(tmpWareHouseDir.toAbsolutePath.toString))
    session.conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    session.conf.set("spark.sql.catalog.local.type", "hadoop")
    session.conf.set("spark.sql.catalog.local.warehouse", tmpWareHouseDir.toAbsolutePath.toString)

    try {
      testFun(session)
    } finally {
      /* empty cache of RDD size, as the referred ids are only valid within a session */
      tearDownSparkSession(session)
    }
  }

  /**
    * @param testFun thunk to run with SparkSession and SparkMonitor as an argument for the tests
    *                that would like to get details on spark jobs submitted
    *
    */
  def withMonitorableSparkSession(testFun: (SparkSession, SparkMonitor) => Any): Unit = {
    val monitor = new SparkMonitor
    val session = setupSparkSession()
    session.sparkContext.addSparkListener(monitor)
    try {
      testFun(session, monitor)
    } finally {
      tearDownSparkSession(session)
    }
  }

  /**
    * @param testFun thunk to run with SparkContext as an argument
    */
  def withSparkContext(testFun: SparkContext => Any) {
    withSparkSession(session => testFun(session.sparkContext))
  }

  /**
    * @param testFun thunk to run with SQLContext as an argument
    */
  def withSparkSqlContext(testFun: SQLContext => Any) {
    withSparkSession(session => testFun(session.sqlContext))
  }

  /**
    * Setups a local sparkSession
    *
    * @return sparkSession to be used
    */
  private def setupSparkSession(wareHouseDir: Option[String] = None) = {
    val sessionBuilder = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", 2.toString)
      .config("spark.sql.adaptive.enabled", value = false)
      .config("spark.driver.bindAddress", "127.0.0.1")

    val session = wareHouseDir.fold(sessionBuilder.getOrCreate())(sessionBuilder
      .config("spark.sql.warehouse.dir", _).getOrCreate())

    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))
    session
  }

  /**
   * to cleanup temp directory used in test
   * @param path - path to cleanup
   */
  private def deleteDirectory(path: Path): Unit = {
    if (Files.exists(path)) {
      Files.walk(path).iterator().toList.reverse.foreach(Files.delete)
    }
  }

  /**
    * Tears down the sparkSession
    *
    * @param session Session to be stopped
    * @return
    */
  private def tearDownSparkSession(session: SparkSession) = {
    session.stop()
    System.clearProperty("spark.driver.port")
    deleteDirectory(tmpWareHouseDir)

  }

}
