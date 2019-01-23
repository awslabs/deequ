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

import java.sql.{Connection, ResultSet}
import java.util
import java.util.{Properties, UUID}

import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzers._
import com.amazon.deequ.analyzers.jdbc.Preconditions._
import com.amazon.deequ.metrics.DoubleMetric
import org.sqlite.Function

import scala.collection.mutable
import scala.io.Source

/** Base class for all analyzers that operate the frequencies of groups in the data */
abstract class JdbcFrequencyBasedAnalyzer(columnsToGroupOn: Seq[String])
  extends JdbcGroupingAnalyzer[JdbcFrequenciesAndNumRows, DoubleMetric] {

  def toDouble(input: String): String = {
    s"CAST($input AS DOUBLE PRECISION)"
  }

  def groupingColumns(): Seq[String] = { columnsToGroupOn }

  override def computeStateFrom(table: Table): Option[JdbcFrequenciesAndNumRows] = {
    Some(JdbcFrequencyBasedAnalyzer.computeFrequencies(table, groupingColumns()))
  }

  /** We need at least one grouping column, and all specified columns must exist */
  override def preconditions: Seq[Table => Unit] = {
    Seq(hasTable()) ++ Seq(atLeastOne(columnsToGroupOn)) ++
      columnsToGroupOn.map { hasColumn } ++ super.preconditions
  }
}


object JdbcFrequencyBasedAnalyzer {

  def computeFrequenciesInSourceDb(
                                    sourceTable: Table,
                                    groupingColumns: Seq[String],
                                    numRows: Option[Long] = None,
                                    targetTable: Table): JdbcFrequenciesAndNumRows = {

    if (sourceTable.jdbcUrl != targetTable.jdbcUrl) {
      throw new IllegalArgumentException("The passed tables must have the same jdbc urls")
    }

    sourceTable.withJdbc { connection: Connection =>

      val columns = groupingColumns.mkString(", ")
      val query =
        s"""
           |CREATE TABLE
           | ${targetTable.name}
           |AS
           | SELECT
           |  $columns, COUNT(*) AS absolute
           | FROM
           |  ${sourceTable.name}
           | GROUP BY
           |  $columns
          """.stripMargin

      val statement = connection.createStatement()
      statement.execute(query)
    }

    JdbcFrequenciesAndNumRows(targetTable, numRows)
  }


  /** Compute the frequencies of groups in the data, essentially via a query like
    *
    * SELECT colA, colB, ..., COUNT(*)
    * FROM DATA
    * WHERE colA IS NOT NULL AND colB IS NOT NULL AND ...
    * GROUP BY colA, colB, ...
    */
  def computeFrequencies(
    table: Table,
    groupingColumns: Seq[String],
    numRows: Option[Long] = None)
  : JdbcFrequenciesAndNumRows = {

    val defaultTable = JdbcFrequencyBasedAnalyzerUtils.newDefaultTable()

    if (table.jdbcUrl == defaultTable.jdbcUrl) {
      return computeFrequenciesInSourceDb(table, groupingColumns, numRows, defaultTable)
    }

    var cols = mutable.LinkedHashMap[String, String]()
    var result: ResultSet = null

    table.withJdbc { connection: Connection =>

      val columns = groupingColumns.mkString(", ")
      val query =
        s"""
           | SELECT $columns, COUNT(*) AS absolute
           |    FROM ${table.name}
           |    GROUP BY $columns
          """.stripMargin

      val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)

      result = statement.executeQuery()

      for (col <- groupingColumns) {
        cols(col) = "TEXT"
      }
      cols("absolute") = "BIGINT"

      val targetTable = Table.create(defaultTable, cols)


      /**
        *
        * @param connection used to write frequencies into targetTable
        * @param result     grouped frequencies
        * @param total      overall number of rows
        * @param numNulls   number of rows with at least one column with a null value
        * @return (total, numNulls)
        */
      def convertResultSet(targetConnection: Connection,
                           result: ResultSet,
                           total: Long, numNulls: Long): (Long, Long) = {

        if (result.next()) {
          var columnValues = Seq[String]()
          val absolute = result.getLong("absolute")

          // only make a map entry if the value is defined for all columns
          for (i <- 1 to groupingColumns.size) {
            val columnValue = Option(result.getObject(i))
            columnValue match {
              case Some(theColumnValue) =>
                columnValues = columnValues :+ theColumnValue.toString
              case None =>
                return convertResultSet(targetConnection, result,
                  total + absolute, numNulls + absolute)
            }
          }

          val query =
            s"""
               | INSERT INTO
               |  ${targetTable.name} (
               |    ${cols.keys.toSeq.mkString(", ")})
               | VALUES
               |  (${columnValues.mkString("'", "', '", "'")}, $absolute)
            """.stripMargin

          val statement = targetConnection.createStatement()
          statement.execute(query)

          convertResultSet(targetConnection, result, total + absolute, numNulls)
        } else {
          (total, numNulls)
        }
      }


      targetTable.withJdbc[JdbcFrequenciesAndNumRows] { targetConnection: Connection =>

        val (numRows, numNulls) = convertResultSet(targetConnection, result, 0, 0)

        val nullValueQuery =
          s"""
             | INSERT INTO
             |  ${targetTable.name} (
             |  ${cols.keys.toSeq.mkString(", ")})
             | VALUES
             |  (${Seq.fill(groupingColumns.size)("null").mkString(", ")}, $numNulls)
        """.stripMargin

        val nullValueStatement = targetConnection.createStatement()
        nullValueStatement.execute(nullValueQuery)

        result.close()

        JdbcFrequenciesAndNumRows(targetTable, Some(numRows), Some(numNulls))
      }
    }
  }
}

/** Base class for all analyzers that compute a (shareable) aggregation over the grouped data */
abstract class JdbcScanShareableFrequencyBasedAnalyzer(name: String, columnsToGroupOn: Seq[String])
  extends JdbcFrequencyBasedAnalyzer(columnsToGroupOn) {

  def aggregationFunctions(numRows: Long): Seq[String]

  override def computeMetricFrom(state: Option[JdbcFrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>

        val aggregations = aggregationFunctions(theState.numRows())

        val result = theState.table.executeAggregations(aggregations)

        fromAggregationResult(result, 0)
      case None =>
        metricFromEmpty(this, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
    }
  }

  override private[deequ] def toFailureMetric(exception: Exception): DoubleMetric = {
    metricFromFailure(exception, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
  }

  protected def toSuccessMetric(value: Double): DoubleMetric = {
    metricFromValue(value, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
  }

  def fromAggregationResult(result: JdbcRow, offset: Int): DoubleMetric = {
    result.getObject(offset) match {
      case null => metricFromEmpty(this, name,
        columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
      case _ => toSuccessMetric(result.getDouble(offset))
    }
  }
}

object JdbcFrequencyBasedAnalyzerUtils {

  def registerSQLiteFunctions(connection: Connection): Unit = {

    // Register user defined function for natural logarithm
    Function.create(connection, "ln", new Function() {
      protected def xFunc(): Unit = {
        result(math.log(value_double(0)))
      }
    })
  }

  def groupingAnalyzerStateStorageConfig(): (String, Properties, Option[Connection => Unit]) = {
    val url = getClass.getResource("/groupingAnalyzerStateStorage.properties")

    def sqLiteConnection(): (String, Properties, Option[Connection => Unit]) = {
      val jdbcUrl = "jdbc:sqlite:groupingAnalyzerTests.db?mode=memory&cache=shared"
      val properties = new Properties()

      (jdbcUrl, properties, Some(registerSQLiteFunctions))
    }

    if (url == null) {
      return sqLiteConnection()
    }

    val properties = new Properties()
    properties.load(Source.fromURL(url).bufferedReader())

    if (!properties.keySet().containsAll(util.Arrays.asList("jdbcUrl", "user", "password"))) {
      return sqLiteConnection()
    }

    val jdbcUrl = properties.getProperty("jdbcUrl")

    (jdbcUrl, properties, None)
  }

  def newDefaultTable(): Table = {

    val (jdbcUrl, properties, onConnect) = groupingAnalyzerStateStorageConfig()
    val tableName = s"__deequ__frequenciesAndNumRows_${UUID.randomUUID().toString
      .replace("-", "")}"

    Table(tableName, jdbcUrl, properties, onConnect)
  }

  private[jdbc] def join(first: Table,
                         second: Table): Table = {

    // TODO: think about deleting old states to keep db clean

    val table = newDefaultTable()
    val columns = first.columns()
    val groupingColumns = columns.keys.toSeq.filter(col => col != "absolute")

    if (columns == second.columns()) {

      table.withJdbc[Table] { connection: Connection =>

        val query =
          s"""
             |CREATE TABLE
             | ${table.name}
             |AS
             | SELECT
             |  ${groupingColumns.mkString(", ")},
             |  SUM(absolute) as absolute
             | FROM (
             |  SELECT * FROM ${first.name}
             |   UNION ALL
             |  SELECT * FROM ${second.name}) AS combinedState
             | GROUP BY
             |  ${groupingColumns.mkString(", ")}
          """.stripMargin

        connection.createStatement().execute(query)
        table
      }
    }
    else {
      throw new IllegalArgumentException("Cannot join tables with different columns")
    }
  }
}
