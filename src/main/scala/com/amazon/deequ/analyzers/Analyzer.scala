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

package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.NullBehavior.NullBehavior
import com.amazon.deequ.analyzers.runners._
import com.amazon.deequ.metrics.FullColumn
import com.amazon.deequ.utilities.ColumnUtil.removeEscapeColumn
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import com.amazon.deequ.metrics.Metric
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import scala.language.existentials
import scala.util.Failure
import scala.util.Success

/**
  * A state (sufficient statistic) computed from data, from which we can compute a metric.
  * Must be combinable with other states of the same type
  * (= algebraic properties of a commutative semi-group)
  */
trait State[S <: State[S]] {

  // Unfortunately this is required due to type checking issues
  private[analyzers] def sumUntyped(other: State[_]): S = {
    sum(other.asInstanceOf[S])
  }

  /** Combine this with another state */
  def sum(other: S): S

  /** Same as sum, syntatic sugar */
  def +(other: S): S = {
    sum(other)
  }
}

/** A state which produces a double valued metric  */
trait DoubleValuedState[S <: DoubleValuedState[S]] extends State[S] {
  def metricValue(): Double
}

/** Common trait for all analyzers which generates metrics from states computed on data frames */
trait Analyzer[S <: State[_], +M <: Metric[_]] extends Serializable {

  /**
    * Compute the state (sufficient statistics) from the data
    * @param data data frame
    * @return
    */
  def computeStateFrom(data: DataFrame): Option[S]

  /**
    * Compute the metric from the state (sufficient statistics)
    * @param state wrapper holding a state of type S (required due to typing issues...)
    * @return
    */
  def computeMetricFrom(state: Option[S]): M

  /**
    * A set of assertions that must hold on the schema of the data frame
    * @return
    */
  def preconditions: Seq[StructType => Unit] = {
    Seq.empty
  }

  /**
    * Runs preconditions, calculates and returns the metric
    *
    * @param data Data frame being analyzed
    * @param aggregateWith loader for previous states to include in the computation (optional)
    * @param saveStatesWith persist internal states using this (optional)
    * @return Returns failure metric in case preconditions fail.
    */
  def calculate(
      data: DataFrame,
      aggregateWith: Option[StateLoader] = None,
      saveStatesWith: Option[StatePersister] = None)
    : M = {

    try {
      preconditions.foreach { condition => condition(data.schema) }

      val state = computeStateFrom(data)

      calculateMetric(state, aggregateWith, saveStatesWith)
    } catch {
      case error: Exception => toFailureMetric(error)
    }
  }

  private[deequ] def toFailureMetric(failure: Exception): M

  def calculateMetric(
      state: Option[S],
      aggregateWith: Option[StateLoader] = None,
      saveStatesWith: Option[StatePersister] = None)
    : M = {

    // Try to load the state
    val loadedState: Option[S] = aggregateWith.flatMap { _.load[S](this) }

    // Potentially merge existing and loaded state
    val stateToComputeMetricFrom: Option[S] = Analyzers.merge(state, loadedState)

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
      sourceA: StateLoader,
      sourceB: StateLoader,
      target: StatePersister)
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

  private[deequ] def loadStateAndComputeMetric(source: StateLoader): Option[M] = {
    source.load[S](this).map { state =>
      computeMetricFrom(Option(state))
    }
  }

  /** Copy the state from source to target. Needs to be here to allow the compiler
    * to correctly infer the types.
    *
    * @param source state provider to read from
    * @param target state provider to write to
    */
  private[deequ] def copyStateTo(source: StateLoader, target: StatePersister): Unit = {
    source.load[S](this).foreach { state => target.persist(this, state) }
  }

}

/** An analyzer that runs a set of aggregation functions over the data,
  * can share scans over the data */
trait ScanShareableAnalyzer[S <: State[_], +M <: Metric[_]] extends Analyzer[S, M] {

  /** Defines the aggregations to compute on the data */
  private[deequ] def aggregationFunctions(): Seq[Column]

  /** Computes the state from the result of the aggregation functions */
  private[deequ] def fromAggregationResult(result: Row, offset: Int): Option[S]

  /** Runs aggregation functions directly, without scan sharing */
  override def computeStateFrom(data: DataFrame): Option[S] = {
    val aggregations = aggregationFunctions()
    val result = data.agg(aggregations.head, aggregations.tail: _*).collect().head
    fromAggregationResult(result, 0)
  }

  /** Produces a metric from the aggregation result */
  private[deequ] def metricFromAggregationResult(
      result: Row,
      offset: Int,
      aggregateWith: Option[StateLoader] = None,
      saveStatesWith: Option[StatePersister] = None)
    : M = {

    val state = fromAggregationResult(result, offset)

    calculateMetric(state, aggregateWith, saveStatesWith)
  }

}

/** A scan-shareable analyzer that produces a DoubleMetric */
abstract class StandardScanShareableAnalyzer[S <: DoubleValuedState[_]](
    name: String,
    instance: String,
    entity: Entity.Value = Entity.Column)
  extends ScanShareableAnalyzer[S, DoubleMetric] {

  override def computeMetricFrom(state: Option[S]): DoubleMetric = {
    state match {
      case Some(theState) =>
        val col = theState match {
          case withColumn: FullColumn => withColumn.fullColumn
          case _ => None
        }
        metricFromValue(theState.metricValue(), name, instance, entity, col)
      case _ =>
        metricFromEmpty(this, name, instance, entity)
    }
  }

  override private[deequ] def toFailureMetric(exception: Exception): DoubleMetric = {
    metricFromFailure(exception, name, instance, entity)
  }

  override def preconditions: Seq[StructType => Unit] = {
    additionalPreconditions() ++ super.preconditions
  }

  protected def additionalPreconditions(): Seq[StructType => Unit] = {
    Seq.empty
  }
}

/** A state for computing ratio-based metrics,
  * contains #rows that match a predicate and overall #rows */
case class NumMatchesAndCount(numMatches: Long, count: Long, override val fullColumn: Option[Column] = None)
  extends DoubleValuedState[NumMatchesAndCount] with FullColumn {

  override def sum(other: NumMatchesAndCount): NumMatchesAndCount = {
    NumMatchesAndCount(numMatches + other.numMatches, count + other.count, sum(fullColumn, other.fullColumn))
  }

  override def metricValue(): Double = {
    if (count == 0L) {
      Double.NaN
    } else {
      numMatches.toDouble / count
    }
  }
}

case class AnalyzerOptions(nullBehavior: NullBehavior = NullBehavior.Ignore)

object NullBehavior extends Enumeration {
  type NullBehavior = Value
  val Ignore, EmptyString, Fail = Value
}

/** Base class for analyzers that compute ratios of matching predicates */
abstract class PredicateMatchingAnalyzer(
    name: String,
    instance: String,
    predicate: Column,
    where: Option[String])
  extends StandardScanShareableAnalyzer[NumMatchesAndCount](name, instance) {

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatchesAndCount] = {

    if (result.isNullAt(offset) || result.isNullAt(offset + 1)) {
      None
    } else {
      val state = NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1))
      Some(state)
    }
  }

  override def aggregationFunctions(): Seq[Column] = {

    val selection = Analyzers.conditionalSelection(predicate, where)

    selection :: count("*") :: Nil
  }
}

/** Base class for analyzers that require to group the data by specific columns */
abstract class GroupingAnalyzer[S <: State[_], +M <: Metric[_]] extends Analyzer[S, M] {

  /** The columns to group the data by */
  def groupingColumns(): Seq[String]

  /** Ensure that the grouping columns exist in the data */
  override def preconditions: Seq[StructType => Unit] = {
    groupingColumns().map { name => Preconditions.hasColumn(name) } ++ super.preconditions
  }
}

/** Helper method to check conditions on the schema of the data */
object Preconditions {

  private[this] val numericDataTypes =
    Set(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)

  private[this] val nestedDataTypes = Set(StructType, MapType, ArrayType)

  private[this] val caseSensitive = {
    SparkSession.builder().getOrCreate()
    .sqlContext.getConf("spark.sql.caseSensitive").equalsIgnoreCase("true")
  }

  def structField(column: String, schema: StructType): StructField = {
    if (caseSensitive) {
      schema(column)
    } else {
      schema.find(_.name.equalsIgnoreCase(removeEscapeColumn(column))).getOrElse {
        throw new IllegalArgumentException(s"Field {$column} doesn't not exist" )
      }
    }
  }
  def hasColumn(column: String, schema: StructType): Boolean = {
    val escapedColumn = removeEscapeColumn(column)
    if (caseSensitive) {
      schema.fieldNames.contains(escapedColumn)
    } else {
      schema.fieldNames.find(_.equalsIgnoreCase(escapedColumn)).isDefined
    }
  }

  /* Return the first (potential) exception thrown by a precondition */
  def findFirstFailing(
      schema: StructType,
      conditions: Seq[StructType => Unit])
    : Option[Exception] = {

    conditions.map { condition =>
      try {
        condition(schema)
        None
      } catch {
        /* We only catch exceptions, not errors! */
        case e: Exception => Some(e)
      }
    }
    .find { _.isDefined }
    .flatten
  }

  /** At least one column is specified */
  def atLeastOne(columns: Seq[String]): StructType => Unit = { _ =>
    if (columns.isEmpty) {
      throw new NoColumnsSpecifiedException("At least one column needs to be specified!")
    }
  }

  /** At least one column is specified */
  def exactlyNColumns(columns: Seq[String], n: Int): StructType => Unit = { _ =>
    if (columns.size != n) {
      throw new NumberOfSpecifiedColumnsException(s"$n columns have to be specified! " +
        s"Currently, columns contains only ${columns.size} column(s): ${columns.mkString(",")}!")
    }
  }

  def isNotNested(column: String): StructType => Unit = { schema =>
    if (hasColumn(column, schema)) {
      val columnDataType = structField(column, schema).dataType
      columnDataType match {
        case _ : StructType | _ : MapType | _ : ArrayType =>
          throw new WrongColumnTypeException(
            s"Unsupported nested column type of column $column: $columnDataType!")
        case _ =>
      }
    }
  }

  /** Specified column exists in the data */
  def hasColumn(column: String): StructType => Unit = { schema =>
    if (!hasColumn(column, schema)) {
      throw new NoSuchColumnException(s"Input data does not include column $column!")
    }
  }

  /** Specified column has a numeric type */
  def isNumeric(column: String): StructType => Unit = { schema =>
    val columnDataType = structField(column, schema).dataType
    val hasNumericType = columnDataType match {
      case ByteType | ShortType | IntegerType | LongType | FloatType |
           DoubleType | _ : DecimalType => true
      case _ => false
    }

    if (!hasNumericType) {
      throw new WrongColumnTypeException(s"Expected type of column $column to be one of " +
        s"(${numericDataTypes.mkString(",")}), but found $columnDataType instead!")
    }
  }

  /** Specified column has string type */
  def isString(column: String): StructType => Unit = { schema =>
    val columnDataType = structField(column, schema).dataType
    val hasStringType = columnDataType match {
      case StringType => true
      case _ => false
    }

    if (!hasStringType) {
      throw new WrongColumnTypeException(s"Expected type of column $column to be " +
        s"StringType, but found $columnDataType instead!")
    }
  }
}

private[deequ] object Analyzers {

  val COL_PREFIX = "com_amazon_deequ_dq_metrics_"
  val COUNT_COL = s"${COL_PREFIX}count"

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
      result: Row,
      offset: Int,
      howMany: Int = 1)
      (func: Unit => S)
    : Option[S] = {

    val nullInResult = (offset until offset + howMany).exists { index => result.isNullAt(index) }

    if (nullInResult) {
      None
    } else {
      Option(func(Unit))
    }
  }

  def entityFrom(columns: Seq[String]): Entity.Value = {
    if (columns.size == 1) Entity.Column else Entity.Multicolumn
  }

  def conditionalSelection(selection: String, where: Option[String]): Column = {
    conditionalSelection(col(selection), where)
  }

  def conditionalSelection(selection: Column, where: Option[String], replaceWith: Double): Column = {
    val conditionColumn = where.map(expr)
    conditionColumn
      .map { condition => when(condition, replaceWith).otherwise(selection) }
      .getOrElse(selection)
  }

  def conditionalSelection(selection: Column, where: Option[String], replaceWith: String): Column = {
    val conditionColumn = where.map(expr)
    conditionColumn
      .map { condition => when(condition, replaceWith).otherwise(selection) }
      .getOrElse(selection)
  }

  def conditionalSelection(selection: Column, condition: Option[String]): Column = {
    val conditionColumn = condition.map { expression => expr(expression) }
    conditionalSelectionFromColumns(selection, conditionColumn)
  }

  private[this] def conditionalSelectionFromColumns(
      selection: Column,
      conditionColumn: Option[Column])
    : Column = {

    conditionColumn
      .map { condition => when(condition, selection) }
      .getOrElse(selection)
  }

  def conditionalCount(where: Option[String]): Column = {
    where
      .map { filter => sum(expr(filter).cast(LongType)) }
      .getOrElse(count("*"))
  }

  def metricFromValue(
      value: Double,
      name: String,
      instance: String,
      entity: Entity.Value = Entity.Column,
      fullColumn: Option[Column] = None)
    : DoubleMetric = {

    DoubleMetric(entity, name, instance, Success(value), fullColumn)
  }

  def emptyStateException(analyzer: Analyzer[_, _]): EmptyStateException = {
    new EmptyStateException(s"Empty state for analyzer $analyzer, all input values were NULL.")
  }

  def metricFromEmpty(
      analyzer: Analyzer[_, _],
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
