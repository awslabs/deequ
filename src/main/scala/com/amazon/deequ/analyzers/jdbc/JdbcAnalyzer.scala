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

import java.sql.ResultSet

import com.amazon.deequ.analyzers.State
import com.amazon.deequ.analyzers.runners.WrongColumnTypeException
import com.amazon.deequ.analyzers.runners.NoSuchColumnException
import com.amazon.deequ.metrics.Metric
import org.postgresql.util.PSQLException


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
    validateTable(table)
    validateColumn(table, column)
  }

  def validateTable(table: Table): Unit = {

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
        throw new Exception(s"The table '${table.name}' does not exist")
    }
    catch {
      case error: PSQLException => throw error
    }
  }

  def validateColumn(table: Table, column: String): Unit = {

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
        throw new NoSuchColumnException(s"The column '$column' does not exist in the table '${table.name}'")
    }
    catch {
      case error: PSQLException => throw error
    }
  }

  def isNumeric(table: Table, column: String): Unit = {
    var numericDataTypes: List[String] = List("smallint", "integer", "bigint", "decimal", "numeric", "real", "double precision", "smallserial", "serial", "bigserial")
    var col_type = columnType(table, column)

    if (!numericDataTypes.contains(col_type))
      throw new WrongColumnTypeException(s"Expected type of column $column to be one of " +
        s"(${numericDataTypes.mkString(",")}), but found $col_type instead!")

  }

  def columnType(table: Table, column: String): String = {
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
      result.next()
      result.getString("data_type")
    }
    catch {
      case error: PSQLException => throw error
    }
  }

}
