package com.amazon.deequ.analyzers.jdbc

import java.sql.{Connection, Statement}
import scala.collection.mutable

trait JdbcFixtureSupport {

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
      "[unique]" -> "TEXT",
      "[nonUnique]" -> "TEXT", "nonUniqueWithNulls" -> "TEXT",
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
}
