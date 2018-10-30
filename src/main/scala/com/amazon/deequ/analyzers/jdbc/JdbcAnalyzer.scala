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
import com.amazon.deequ.analyzers.runners.{NoSuchColumnException, NoSuchTableException, WrongColumnTypeException}
import com.amazon.deequ.metrics.Metric
import org.postgresql.util.PSQLException
import java.sql.Types._

import Preconditions.{hasColumn, hasTable}


trait JdbcAnalyzer[S <: State[_], +M <: Metric[_]] {

  def computeStateFrom(table: Table): Option[S]

  def computeMetricFrom(state: Option[S]): M

  def calculate(table: Table): M = {

    try {
      val state = computeStateFrom(table)
      computeMetricFrom(state)
    } catch {
      case error: Exception => toFailureMetric(error)
    }
  }

  private[deequ] def toFailureMetric(failure: Exception): M

  def validateParams(table: Table, column: String): Unit = {
    hasTable(table)
    hasColumn(table, column)
  }
}

/** Helper method to check conditions on the schema of the data */
object Preconditions {

  private[this] val numericDataTypes =
    Set(BIGINT, DECIMAL, DOUBLE, FLOAT, INTEGER, NUMERIC, SMALLINT, TINYINT)

  /** Specified table exists in the data */
  def hasTable(table: Table): Unit = {

    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | *
         |FROM
         | INFORMATION_SCHEMA.TABLES
         |WHERE
         | TABLE_NAME = ?
        """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
      ResultSet.CONCUR_READ_ONLY)

    statement.setString(1, table.name)

    val result = statement.executeQuery()

    try {
      if (!result.first())
        throw new NoSuchTableException(s"Input data does not include table ${table.name}!")
    }
    catch {
      case error: PSQLException => throw error
    }
  }

  /** Specified column exists in the table */
  def hasColumn(table: Table, column: String): Unit = {
    val connection = table.jdbcConnection

    val query =
      s"""
         |SELECT
         | *
         |FROM
         | INFORMATION_SCHEMA.COLUMNS
         |WHERE
         | TABLE_NAME = ?
         |AND
         | COLUMN_NAME = ?
      """.stripMargin

    val statement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
      ResultSet.CONCUR_READ_ONLY)

    statement.setString(1, table.name)
    statement.setString(2, column)

    val result = statement.executeQuery()

    try {
      if (!result.first())
        throw new NoSuchColumnException(s"Input data does not include column $column!")
    }
    catch {
      case error: PSQLException => throw error
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

    val statement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
      ResultSet.CONCUR_READ_ONLY)

    val result = statement.executeQuery()

    try {
      val metaData = result.getMetaData
      metaData.getColumnType(1)
    }
    catch {
      case error: PSQLException => throw error
    }
  }

  /** Specified column has a numeric type */
  def isNumeric(table: Table, column: String): Unit = {
    val columnDataType = getColumnDataType(table, column)

    val hasNumericType = numericDataTypes.contains(columnDataType)

    if (!hasNumericType) {
      throw new WrongColumnTypeException(s"Expected type of column $column to be one of " +
        s"(${numericDataTypes.map(t => JDBCType.valueOf(t).getName).mkString(",")}), but found ${JDBCType.valueOf(columnDataType).getName} instead!")
    }
  }
}
