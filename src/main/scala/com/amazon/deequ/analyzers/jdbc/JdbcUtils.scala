package com.amazon.deequ.analyzers.jdbc

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.io.Source

private[jdbc] object JdbcUtils {

  classOf[org.postgresql.Driver]

  val jdbcUrl = "jdbc:postgresql://localhost:5432/food"

  def connectionProperties(): Properties = {

    val url = getClass.getResource("/jdbc.properties")

    if (url == null) {
      throw new IllegalStateException("Unable to find jdbc.properties in src/main/resources!")
    }

    val properties = new Properties()
    properties.load(Source.fromURL(url).bufferedReader())

    properties
  }

  def withJdbc(func: Connection => Unit): Unit = {
    classOf[org.postgresql.Driver]
    val connection = DriverManager.getConnection(jdbcUrl, connectionProperties())
    try {
      func(connection)
    } finally {
      connection.close()
    }
  }

  def withSpark(func: SparkSession => Unit): Unit = {
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
