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

import java.sql.Types._
import java.sql.{JDBCType, ResultSet}

import com.amazon.deequ.analyzers.runners._
import com.amazon.deequ.analyzers.{DoubleValuedState, State}
import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}

import scala.util.{Failure, Success}

/** Common trait for all analyzers which generates metrics from states computed on relations */
trait JdbcAnalyzer[S <: State[_], +M <: Metric[_]] {

  /**
    * Compute the state (sufficient statistics) from the data
    * @param data database table
    * @return
    */
  def computeStateFrom(data: Table): Option[S]

  /**
    * Compute the metric from the state (sufficient statistics)
    * @param state wrapper holding a state of type S (required due to typing issues...)
    * @return
    */
  def computeMetricFrom(state: Option[S]): M

  /**
    * A set of assertions that must hold on the schema of the database table
    * @return
    */
  def preconditions: Seq[Table => Unit] = {
    Preconditions.hasTable() :: Nil
  }

  // TODO: This function is not defined in Analyzer.scala
  def validatePreconditions(data: Table): Unit = {
    val exceptionOption = Preconditions.findFirstFailing(data, this.preconditions)
    if (exceptionOption.nonEmpty) {
      throw exceptionOption.get
    }
  }

  /**
    * Runs preconditions, calculates and returns the metric
    *
    * @param data Database table being analyzed
    * @param aggregateWith loader for previous states to include in the computation (optional)
    * @param saveStatesWith persist internal states using this (optional)
    * @return Returns failure metric in case preconditions fail.
    */
  def calculate(
      data: Table,
      aggregateWith: Option[JdbcStateLoader] = None,
      saveStatesWith: Option[JdbcStatePersister] = None)
    : M = {

    try {
      validatePreconditions(data)
      val state = computeStateFrom(data)
      calculateMetric(state, aggregateWith, saveStatesWith)
    } catch {
      case error: Exception => toFailureMetric(error)
    }
  }

  private[deequ] def toFailureMetric(failure: Exception): M

  protected def calculateMetric(
      state: Option[S],
      aggregateWith: Option[JdbcStateLoader] = None,
      saveStatesWith: Option[JdbcStatePersister] = None)
  : M = {

    // Try to load the state
    val loadedState: Option[S] = aggregateWith.flatMap { _.load[S](this) }

    // Potentially merge existing and loaded state
    val stateToComputeMetricFrom: Option[S] = JdbcAnalyzers.merge(state, loadedState)

    // Persist the state if it is not empty and a persister was provided
    stateToComputeMetricFrom
      .foreach { state =>
        saveStatesWith.foreach {
          _.persist[S](this, state)
        }
      }

    computeMetricFrom(stateToComputeMetricFrom)
  }

  private[deequ] def aggregateStateTo(
      sourceA: JdbcStateLoader,
      sourceB: JdbcStateLoader,
      target: JdbcStatePersister)
  : Unit = {

    val maybeStateA = sourceA.load[S](this)
    val maybeStateB = sourceB.load[S](this)

    val aggregated = (maybeStateA, maybeStateB) match {
      case (Some(stateA), Some(stateB)) => Some(stateA.sumUntyped(stateB).asInstanceOf[S])
      case (Some(stateA), None) => Some(stateA)
      case (None, Some(stateB)) => Some(stateB)
      case _ => None
    }

    aggregated.foreach { state => target.persist[S](this, state) }
  }

  private[deequ] def loadStateAndComputeMetric(source: JdbcStateLoader): Option[M] = {
    source.load[S](this).map { state =>
      computeMetricFrom(Option(state))
    }
  }
}

/** An analyzer that runs a set of aggregation functions over the data,
  * can share scans over the data */
trait JdbcScanShareableAnalyzer[S <: State[_], +M <: Metric[_]] extends JdbcAnalyzer[S, M] {

  /** Defines the aggregations to compute on the data */
  private[deequ] def aggregationFunctions(): Seq[String]

  /** Computes the state from the result of the aggregation functions */
  private[deequ] def fromAggregationResult(result: JdbcRow, offset: Int): Option[S]

  /** Runs aggregation functions directly, without scan sharing */
  override def computeStateFrom(data: Table): Option[S] = {
    val aggregations = aggregationFunctions()
    val result = data.executeAggregations(aggregations)
    val state = fromAggregationResult(result, 0)
    state
  }

  /** Produces a metric from the aggregation result */
  private[deequ] def metricFromAggregationResult(
      result: JdbcRow,
      offset: Int,
      aggregateWith: Option[JdbcStateLoader] = None,
      saveStatesWith: Option[JdbcStatePersister] = None)
  : M = {

    val state = fromAggregationResult(result, offset)

    calculateMetric(state, aggregateWith, saveStatesWith)
  }

  override def preconditions: Seq[Table => Unit] = {
    additionalPreconditions() ++ super.preconditions
  }

  protected def additionalPreconditions(): Seq[Table => Unit] = {
    Seq.empty
  }

}

/** A scan-shareable analyzer that produces a DoubleMetric */
abstract class JdbcStandardScanShareableAnalyzer[S <: DoubleValuedState[_]](
     name: String,
     instance: String,
     entity: Entity.Value = Entity.Column)
  extends JdbcScanShareableAnalyzer[S, DoubleMetric] {

  override def computeMetricFrom(state: Option[S]): DoubleMetric = {
    state match {
      case Some(theState) =>
        JdbcAnalyzers.metricFromValue(theState.metricValue(), name, instance, entity)
      case _ =>
        JdbcAnalyzers.metricFromEmpty(this, name, instance, entity)
    }
  }

  override private[deequ] def toFailureMetric(exception: Exception): DoubleMetric = {
    JdbcAnalyzers.metricFromFailure(exception, name, instance, entity)
  }
}

/** Base class for analyzers that require to group the data by specific columns */
abstract class JdbcGroupingAnalyzer[S <: State[_], +M <: Metric[_]] extends JdbcAnalyzer[S, M] {

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

    table.withJdbc { connection =>

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
  }

  /** At least one column is specified */
  def atLeastOne(columns: Seq[String]): Table => Unit = { _ =>
    if (columns.isEmpty) {
      throw new NoColumnsSpecifiedException("At least one column needs to be specified!")
    }
  }

  /** Exactly n columns are specified */
  def exactlyNColumns(columns: Seq[String], n: Int): Table => Unit = { _ =>
    if (columns.size != n) {
      throw new NumberOfSpecifiedColumnsException(s"$n columns have to be specified! " +
        s"Currently, columns contains only ${columns.size} column(s): ${columns.mkString(",")}!")
    }
  }

  /** Specified column exists in the table */
  def hasColumn(column: String): Table => Unit = { table =>


    table.withJdbc { connection =>

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
  }

  /** data type of specified column */
  def getColumnDataType(table: Table, column: String): Int = {


    table.withJdbc { connection =>

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
    val st = statement.getOrElse("")
    if (!st.matches(pattern)) {
      throw new SQLInjectionException(
        s"In the statement semicolons outside of quotation marks are not allowed")
    }
  }
}

private[deequ] object JdbcAnalyzers {

  /** Merges a sequence of potentially empty states. */
  def merge[S <: State[_]](
      state: Option[S],
      anotherState: Option[S],
      moreStates: Option[S]*)
  : Option[S] = {

    val statesToMerge = Seq(state, anotherState) ++ moreStates

    statesToMerge.reduce { (stateA: Option[S], stateB: Option[S]) =>

      (stateA, stateB) match {
        case (Some(theStateA), Some(theStateB)) =>
          Some(theStateA.sumUntyped(theStateB).asInstanceOf[S])

        case (Some(_), None) => stateA
        case (None, Some(_)) => stateB
        case _ => None
      }
    }
  }

  /** Tests whether the result columns from offset to offset + howMany are non-null */
  def ifNoNullsIn[S <: State[_]](
      result: JdbcRow,
      offset: Int,
      howMany: Int = 1)
      (func: Unit => S)
  : Option[S] = {

    val nullInResult = (offset until offset + howMany).exists { index =>
      result.getObject(index) == null }

    if (nullInResult) {
      None
    } else {
      Option(func(Unit))
    }
  }

  def entityFrom(columns: Seq[String]): Entity.Value = {
    if (columns.size == 1) Entity.Column else Entity.Mutlicolumn
  }

  def conditionalSelection(column: String, where: Option[String]): String = {
    where
      .map { condition => s"CASE WHEN ($condition) THEN $column ELSE NULL END" }
      .getOrElse(column)
  }

  def conditionalSelectionNotNull(column: String, where: Option[String]): String = {

    conditionalSelection(column, where :: Some(s"$column IS NOT NULL") :: Nil)
  }

  def conditionalNotNull(firstColumn: String, secondColumn: String, where: Option[String],
                         consequent: String): String = {
    where
      .map { filter =>
        s"CASE WHEN ($firstColumn IS NOT NULL AND $secondColumn IS NOT NULL AND $filter) THEN " +
          s"$consequent ELSE NULL END" }
      .getOrElse(s"CASE WHEN ($firstColumn IS NOT NULL AND $secondColumn IS NOT NULL) THEN " +
        s"$consequent ELSE NULL END")
  }

  def conditionalSelection(column: String, where: Seq[Option[String]]): String = {

    val whereConcat = where
      .map(whereOption => whereOption
        .map(condition => condition)
        .getOrElse("TRUE=TRUE"))

    conditionalSelection(column, Some(whereConcat.mkString(" AND ")))
  }

  def conditionalCount(where: Option[String]): String = {
    where
      .map { filter => s"SUM(CASE WHEN ($filter) THEN 1 ELSE 0 END)" }
      .getOrElse("COUNT(*)")
  }

  def conditionalCountNotNull(column : String, where: Option[String]): String = {
    where
      .map { filter => s"SUM(CASE WHEN ($filter) AND $column IS NOT NULL THEN 1 ELSE 0 END)" }
      .getOrElse(s"COUNT($column)")
  }

  def metricFromValue(
      value: Double,
      name: String,
      instance: String,
      entity: Entity.Value = Entity.Column)
    : DoubleMetric = {

    DoubleMetric(entity, name, instance, Success(value))
  }

  def emptyStateException(analyzer: JdbcAnalyzer[_, _]): EmptyStateException = {
    new EmptyStateException(s"Empty state for analyzer $analyzer, all input values were NULL.")
  }

  def metricFromEmpty(
      analyzer: JdbcAnalyzer[_, _],
      name: String,
      instance: String,
      entity: Entity.Value = Entity.Column)
    : DoubleMetric = {
    metricFromFailure(emptyStateException(analyzer), name, instance, entity)
  }

  def metricFromFailure(
      exception: Throwable,
      name: String,
      instance: String,
      entity: Entity.Value = Entity.Column)
    : DoubleMetric = {

    DoubleMetric(entity, name, instance, Failure(
      MetricCalculationException.wrapIfNecessary(exception)))
  }
}
