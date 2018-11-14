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

import java.sql.{JDBCType, ResultSet}

import com.amazon.deequ.analyzers.State
import com.amazon.deequ.analyzers.runners.{NoSuchColumnException, NoSuchTableException, WrongColumnTypeException, SQLInjectionException}
import com.amazon.deequ.metrics.Metric
import java.sql.Types._

trait JdbcAnalyzer[S <: State[_], +M <: Metric[_]] {

  /**
    * Compute the state (sufficient statistics) from the data
    * @param table database table
    * @return
    */
  def computeStateFrom(table: Table): Option[S]

  /**
    * Compute the metric from the state (sufficient statistics)
    * @param state wrapper holding a state of type S (required due to typing issues...)
    * @return
    */
  def computeMetricFrom(state: Option[S]): M

  /**
    * A set of assertions that must hold on the database table
    * @return
    */
  def preconditions: Seq[Table => Unit] = {
    Seq.empty
  }

  def validatePreconditions(table: Table): Unit = {
    val exceptionOption = Preconditions.findFirstFailing(table, this.preconditions)
    if (exceptionOption.nonEmpty) {
      throw exceptionOption.get
    }
  }

  def calculate(table: Table): M = {

    try {
      validatePreconditions(table)
      val state = computeStateFrom(table)
      computeMetricFrom(state)
    } catch {
      case error: Exception => toFailureMetric(error)
    }
  }

  private[deequ] def toFailureMetric(failure: Exception): M
}

/** Base class for analyzers that require to group the data by specific columns */
abstract class GroupingAnalyzer[S <: State[_], +M <: Metric[_]] extends JdbcAnalyzer[S, M] {

  /** The columns to group the data by */
  def groupingColumns(): Seq[String]

  /** Ensure that the grouping columns exist in the data */
  override def preconditions: Seq[Table => Unit] = {
    groupingColumns().map { name => Preconditions.hasColumn(name) } ++ super.preconditions
  }
}

/** Helper method to check conditions on the schema of the data */
object Preconditions {

  private[this] val numericDataTypes =
    Set(BIGINT, DECIMAL, DOUBLE, FLOAT, INTEGER, NUMERIC, SMALLINT, TINYINT)

  /* Return the first (potential) exception thrown by a precondition */
  def findFirstFailing(
                        table: Table,
                        conditions: Seq[Table => Unit])
  : Option[Exception] = {

    conditions.map { condition =>
      try {
        condition(table)
        None
      } catch {
        /* We only catch exceptions, not errors! */
        case e: Exception => Some(e)
      }
    }
      .find { _.isDefined }
      .flatten
  }

  /** Specified table exists in the data */
  def hasTable(): Table => Unit = { table =>

    val connection = table.jdbcConnection

    val metaData = connection.getMetaData
    val result = metaData.getTables(null, null, null, Array[String]("TABLE"))

    var hasTable = false

    while (result.next()) {
      if (result.getString("TABLE_NAME") == table.name) {
        hasTable = true
      }
    }

    if (!hasTable) {
      throw new NoSuchTableException(s"Input data does not include table ${table.name}!")
    }
  }

  /** Specified column exists in the table */
  def hasColumn(column: String): Table => Unit = { table =>

    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | *
         |FROM
         | ${table.name}
         |LIMIT 0
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()
    val metaData = result.getMetaData

    var hasColumn = false

    for (i <- 1 to metaData.getColumnCount) {
      if (metaData.getColumnName(i) == column) {
        hasColumn = true
      }
    }

    if (!hasColumn) {
      throw new NoSuchColumnException(s"Input data does not include column $column!")
    }
  }

  /** data type of specified column */
  def getColumnDataType(table: Table, column: String): Int = {
    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | $column
         |FROM
         | ${table.name}
         |LIMIT 0
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    val metaData = result.getMetaData
    metaData.getColumnType(1)
  }

  /** Specified column has a numeric type */
  def isNumeric(column: String): Table => Unit = { table =>
    val columnDataType = getColumnDataType(table, column)

    val hasNumericType = numericDataTypes.contains(columnDataType)

    if (!hasNumericType) {
      throw new WrongColumnTypeException(s"Expected type of column $column to be one of " +
        s"(${numericDataTypes.map(t => JDBCType.valueOf(t).getName).mkString(",")}), " +
        s"but found ${JDBCType.valueOf(columnDataType).getName} instead!")
    }
  }

  /** Statement has no ; outside of quotation marks (SQL injections) */
  def hasNoInjection(statement: Option[String]): Table => Unit = { _ =>
    val pattern = """("[^"]*"|'[^']*'|[^;'"]*)*"""
    val st = statement.get
    if (!st.matches(pattern)) {
      throw new SQLInjectionException(
        s"In the statement semicolons outside of quotation marks are not allowed")
    }
  }
}
