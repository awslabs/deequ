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

package com.amazon.deequ.analyzers.jdbc

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import scala.collection.mutable

case class Table (name: String,
                  jdbcUrl: String,
                  jdbcConnectionProperties: Properties,
                  onConnect: Option[Connection => Unit] = None) {


  /**
    * just create a connection when needed
    * @return the new opened connection
    */
  private def createConnection(): Connection = {

    val connection = DriverManager.getConnection(jdbcUrl, jdbcConnectionProperties)

    onConnect match {
      case Some(theOnConnectFunc) => theOnConnectFunc(connection)
      case _ =>
    }

    connection
  }


  private[jdbc] def withJdbc[T](func: Connection => T): T = {

    val connection = createConnection()

    try {
      func(connection)
    } finally {
      connection.close()
    }
  }


  /**
    * Builds and executes SQL statement and returns the ResultSet
    *
    * @param aggregations Sequence of aggregation functions
    * @return Returns ResultSet of the query
    */
  def executeAggregations(aggregations: Seq[String]): JdbcRow = {

    withJdbc[JdbcRow] { connection: Connection =>

      val query =
        s"""
           |SELECT
           | ${aggregations.mkString(", ")}
           |FROM
           | $name
        """.stripMargin

      val result = connection.createStatement().executeQuery(query)
      // TODO: Test return value of next() and throw exception
      result.next()
      JdbcRow.from(result)
    }
  }


  private[jdbc] def columns(): mutable.LinkedHashMap[String, String] = {


    withJdbc[mutable.LinkedHashMap[String, String]] { connection: Connection =>

      val query =
        s"""
           |SELECT
           | *
           |FROM
           | $name
           |LIMIT 0
          """.stripMargin

      val result = connection.createStatement().executeQuery(query)

      // TODO: Test return value of next() and throw exception
      val metaData = result.getMetaData
      val colCount = metaData.getColumnCount

      var cols = mutable.LinkedHashMap[String, String]()
      for (col <- 1 to colCount) {
        cols(metaData.getColumnLabel(col)) = metaData.getColumnTypeName(col)
      }

      cols
    }
  }
}



object Table {

  def create(table: Table,
             columns: mutable.LinkedHashMap[String, String]): Table = {

    table.withJdbc[Table] { connection: Connection =>

      val deletionQuery =
        s"""
           |DROP TABLE IF EXISTS
           | ${table.name}
        """.stripMargin

      val stmt = connection.createStatement()
      stmt.execute(deletionQuery)

      val creationQuery =
        s"""
           |CREATE TABLE
           | ${table.name}
           |  ${columns map { case (key, value) => s"$key $value" } mkString("(", ",", ")")}
       """.stripMargin

      stmt.execute(creationQuery)

      table
    }
  }

  def fill(table: Table,
           columns: mutable.LinkedHashMap[String, String],
           frequencies: Map[Seq[String], Long]): Table = {

    if (frequencies.nonEmpty) {

      table.withJdbc { connection: Connection =>

        val values = frequencies.map(entry =>
          s"(${entry._1.mkString("'", "', '", "'")}, '${entry._2}')").mkString(", ")

        val query =
          s"""
             |INSERT INTO
             | ${table.name} ${columns.keys.mkString("(", ", ", ")")}
             |VALUES
             | $values
         """.stripMargin

        val stmt = connection.createStatement()
        stmt.execute(query)
      }
    }

    table
  }

  def createAndFill(table: Table,
                    columns: mutable.LinkedHashMap[String, String],
                    frequencies: Map[Seq[String], Long]): Table = {

    create(table, columns)
    fill(table, columns, frequencies)
  }
}


case class JdbcRow(row: Seq[Any]) {

  def getLong(col: Int): Long = {

    row(col) match {
      case number: Number => number.longValue()
      case null => 0
      case _ => throw new IllegalArgumentException("No numeric type")
    }
  }

  def getDouble(col: Int): Double = {

    row(col) match {
      case number: Number => number.doubleValue()
      case null => 0.0
      case _ => throw new IllegalArgumentException("No numeric type")
    }
  }

  def getObject(col: Int): Any = {
    row(col)
  }
}

object JdbcRow {

  def from(resultSet: ResultSet): JdbcRow = {
    var row = Seq.empty[Object]
    val numColumns = resultSet.getMetaData.getColumnCount

    for (col <- 1 to numColumns) {
      row = row :+ resultSet.getObject(col)
    }

    JdbcRow(row)
  }
}
