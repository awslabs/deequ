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

import java.sql.{Connection, Statement}
import scala.collection.mutable

trait JdbcFixtureSupport {

  def hasTable(conn: Connection, tableName: String): Boolean = {
    val query =
      s"""
         |SELECT
         |  name
         |FROM
         |  sqlite_master
         |WHERE
         |  type='table'
         |  AND name='$tableName'
       """.stripMargin

    val stmt = conn.createStatement()
    val result = stmt.executeQuery(query)

    if (result.next()) {
      true
    } else {
      false
    }
  }

  def fillTableWithData(conn: Connection, tableName: String,
                        columns: mutable.LinkedHashMap[String, String], values: Seq[Seq[Any]]
                       ): Table = {

    val deletionQuery =
      s"""
         |DROP TABLE IF EXISTS
         | $tableName
       """.stripMargin

    val stmt = conn.createStatement()
    stmt.execute(deletionQuery)

    val creationQuery =
      s"""
         |CREATE TABLE IF NOT EXISTS
         | $tableName
         |  ${columns map { case (key, value) => s"$key $value" } mkString("(", ",", ")")}
       """.stripMargin

    stmt.execute(creationQuery)

    if (values.nonEmpty) {
      val sqlValues = values.map(row => {
        row.map({
          case value: String => "\"" + value + "\""
          case value => s"$value"
        }).mkString("(", ",", ")")
      }).mkString(",")

      val insertQuery =
        s"""
           |INSERT INTO $tableName
           | ${columns.keys.mkString("(", ",", ")")}
           |VALUES
           | $sqlValues
         """.stripMargin

      stmt.execute(insertQuery)
    }

    Table(tableName, conn)
  }


  def getTableMissingColumnWithSize(conn: Connection): (Table, Long) = {
    val columns = mutable.LinkedHashMap[String, String]("item" -> "INTEGER", "att1" -> "INTEGER")
    val data =
      Seq(
        Seq(1, null),
        Seq(2, null),
        Seq(3, null))

    (fillTableWithData(conn, "MissingColumn", columns, data), data.size)
  }

  def getTableMissingColumn(conn: Connection): Table = {
    getTableMissingColumnWithSize(conn)._1
  }

  def getTableEmptyWithSize(conn: Connection): (Table, Long) = {
    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "INTEGER", "att1" -> "INTEGER")

    val data = Seq()

    (fillTableWithData(conn, "EmptyTable", columns, data), 0)
  }

  def getTableEmpty(conn: Connection): Table = {
    getTableEmptyWithSize(conn)._1
  }

  def getTableMissingWithSize(conn: Connection): (Table, Long) = {

    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "TEXT", "att1" -> "TEXT", "att2" -> "TEXT")

    val data =
      Seq(
        Seq("1", "a", "f"),
        Seq("2", "b", "d"),
        Seq("3", null, "f"),
        Seq("4", "a", null),
        Seq("5", "a", "f"),
        Seq("6", null, "d"),
        Seq("7", null, "d"),
        Seq("8", "b", null),
        Seq("9", "a", "f"),
        Seq("10", null, null),
        Seq("11", null, "f"),
        Seq("12", null, "d")
      )
    (fillTableWithData(conn, "Missing", columns, data), data.size)
  }

  def getTableMissing(conn: Connection): Table = {
    getTableMissingWithSize(conn)._1
  }

  def getTableFullWithSize(conn: Connection): (Table, Long) = {

    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "TEXT", "att1" -> "TEXT", "att2" -> "TEXT")

    val data =
      Seq(
        Seq("1", "a", "c"),
        Seq("2", "a", "c"),
        Seq("3", "a", "c"),
        Seq("4", "b", "d")
      )
    (fillTableWithData(conn, "Full", columns, data), data.size)
  }

  def getTableFull(conn: Connection): Table = {
    getTableFullWithSize(conn)._1
  }

  def getTableWithNegativeNumbers(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "TEXT", "att1" -> "TEXT", "att2" -> "TEXT")

    val data =
      Seq(
        Seq("1", "-1", "-1.0"),
        Seq("2", "-2", "-2.0"),
        Seq("3", "-3", "-3.0"),
        Seq("4", "-4", "-4.0")
      )
    fillTableWithData(conn, "NegativeNumbers", columns, data)
  }

  def getTableCompleteAndInCompleteColumns(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "TEXT", "att1" -> "TEXT", "att2" -> "TEXT")

    val data =
      Seq(
        Seq("1", "a", "f"),
        Seq("2", "b", "d"),
        Seq("3", "a", null),
        Seq("4", "a", "f"),
        Seq("5", "b", null),
        Seq("6", "a", "f")
      )
    fillTableWithData(conn, "CompleteAndInCompleteColumns", columns, data)
  }

  def getTableCompleteAndInCompleteColumnsDelta(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "TEXT", "att1" -> "TEXT", "att2" -> "TEXT")

    val data =
      Seq(
        Seq("7", "a", null),
        Seq("8", "b", "d"),
        Seq("9", "a", null)
      )
    fillTableWithData(conn, "CompleteAndInCompleteColumns", columns, data)
  }

  def getTableFractionalIntegralTypes(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "TEXT", "att1" -> "TEXT")

    val data =
      Seq(
        Seq("1", "1.0"),
        Seq("2", "1")
      )
    fillTableWithData(conn, "FractionalIntegralTypes", columns, data)
  }

  def getTableFractionalStringTypes(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String]("item" -> "TEXT", "att1" -> "TEXT")
    val data =
      Seq(
        Seq("1", "1.0"),
        Seq("2", "a")
      )
    fillTableWithData(conn, "FractionalStringTypes", columns, data)
  }

  def getTableIntegralStringTypes(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String]("item" -> "TEXT", "att1" -> "TEXT")
    val data =
      Seq(
        Seq("1", "1"),
        Seq("2", "a")
      )
    fillTableWithData(conn, "IntegralStringTypes", columns, data)
  }

  def getTableWithNumericValues(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "TEXT", "att1" -> "INTEGER", "att2" -> "INTEGER")

    val data =
      Seq(
        Seq("1", 1, 0),
        Seq("2", 2, 0),
        Seq("3", 3, 0),
        Seq("4", 4, 5),
        Seq("5", 5, 6),
        Seq("6", 6, 7)
      )
    fillTableWithData(conn, "NumericValues", columns, data)
  }

  def getTableWithNumericFractionalValues(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "TEXT", "att1" -> "INTEGER", "att2" -> "INTEGER")

    val data =
      Seq(
        Seq("1", 1.0, 0.0),
        Seq("2", 2.0, 0.0),
        Seq("3", 3.0, 0.0),
        Seq("4", 4.0, 5.0),
        Seq("5", 5.0, 6.0),
        Seq("6", 6.0, 7.0)
      )
    fillTableWithData(conn, "NumericFractionalValues", columns, data)
  }

  def getTableWithUniqueColumns(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String](
      "uniqueValues" -> "TEXT",
      "nonUniqueValues" -> "TEXT", "nonUniqueWithNulls" -> "TEXT",
      "uniqueWithNulls" -> "TEXT",
      "onlyUniqueWithOtherNonUnique" -> "TEXT",
      "halfUniqueCombinedWithNonUnique" -> "TEXT")

    val data =
      Seq(
        Seq("1", "0", "3", "1", "5", "0"),
        Seq("2", "0", "3", "2", "6", "0"),
        Seq("3", "0", "3", null, "7", "0"),
        Seq("4", "5", null, "3", "0", "4"),
        Seq("5", "6", null, "4", "0", "5"),
        Seq("6", "7", null, "5", "0", "6")
      )
    fillTableWithData(conn, "UniqueColumns", columns, data)
  }

  def getTableWithDistinctValues(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String]("att1" -> "TEXT", "att2" -> "TEXT")
    val data =
      Seq(
        Seq("a", null),
        Seq("a", null),
        Seq(null, "x"),
        Seq("b", "x"),
        Seq("b", "x"),
        Seq("c", "y")
      )
    fillTableWithData(conn, "DistinctValues", columns, data)
  }

  def getTableWithEmptyStringValues(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String]("att1" -> "TEXT", "att2" -> "TEXT")
    val data =
      Seq(
        Seq("1", ""),
        Seq("2", ""),
        Seq("3", "x"),
        Seq("4", "x"),
        Seq("5", "x"),
        Seq("6", "")
      )
    fillTableWithData(conn, "EmptyStringValues", columns, data)
  }

  def getTableWithWhitespace(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String]("att1" -> "TEXT", "att2" -> "TEXT")
    val data =
      Seq(
        Seq("1", "x"),
        Seq("2", " "),
        Seq("3", " "),
        Seq("4", " "),
        Seq("5", "x"),
        Seq("6", "x")
      )
    fillTableWithData(conn, "Whitespace", columns, data)
  }

  def getTableWithConditionallyUninformativeColumns(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String]("att1" -> "INTEGER", "att2" -> "INTEGER")
    val data =
      Seq(
        Seq(1, 0),
        Seq(2, 0),
        Seq(3, 0)
      )
    fillTableWithData(conn, "ConditionallyUninformativeColumns", columns, data)
  }

  def getTableWithConditionallyInformativeColumns(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String]("att1" -> "INTEGER", "att2" -> "INTEGER")
    val data =
      Seq(
        Seq(1, 4),
        Seq(2, 5),
        Seq(3, 6)
      )
    fillTableWithData(conn, "ConditionallyInformativeColumns", columns, data)
  }

  def getTableWithImproperDataTypes(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String](
      "mixed" -> "TEXT", "type_integer" -> "INTEGER", "type_fractional" -> "DOUBLE")

    val data =
      Seq(
        Seq("\n\n-1  ", 1, 2.3),
        Seq("\n+2.376  \n", 2, 5.6),
        Seq("true", 3, null),
        Seq("null", null, null),
        Seq("string", 6, 3.3),
        Seq("null", 6, 3.3)
      )
    fillTableWithData(conn, "MissingTypes", columns, data)
  }

  def getTableWithInverseNumberedColumns(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String]("att1" -> "INTEGER", "att2" -> "INTEGER")
    val data =
      Seq(
        Seq(1, 6),
        Seq(2, 5),
        Seq(3, 4)
      )
    fillTableWithData(conn, "ConditionallyInformativeColumns", columns, data)
  }

  def getTableWithPartlyCorrelatedColumns(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String]("att1" -> "INTEGER", "att2" -> "INTEGER")
    val data =
      Seq(
        Seq(1, 4),
        Seq(2, 2),
        Seq(3, 6)
      )
    fillTableWithData(conn, "ConditionallyInformativeColumns", columns, data)
  }

  def getTableWithPricedItems(conn: Connection): Table = {

    val columns = mutable.LinkedHashMap[String, String](
      "item" -> "TEXT", "att1" -> "TEXT", "count" -> "INTEGER", "price" -> "REAL")
    val data =
      Seq(
        Seq("1", "a", 17, 1.3),
        Seq("2", null, 12, 76.0),
        Seq("3", "b", 15, 89.0),
        Seq("4", "b", 12, 12.7),
        Seq("5", null, 1, 1.0),
        Seq("6", "a", 21, 78.0),
        Seq("7", null, 12, 0.0)
      )
    fillTableWithData(conn, "PricedItems", columns, data)
  }
}
