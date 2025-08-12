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

import com.amazon.deequ.analyzers.Histogram.{AggregateFunction, Count}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Histogram is the summary of values in a column of a DataFrame. Groups the given column's values,
 * and calculates either number of rows or with that specific value and the fraction of this value or
 * sum of values in other column.
 *
 * @param column        Column to do histogram analysis on
 * @param binningUdf    Optional binning function to run before grouping to re-categorize the
 *                      column values.
 *                      For example to turn a numerical value to a categorical value binning
 *                      functions might be used.
 * @param maxDetailBins Histogram details is only provided for N column values with top counts.
 *                      maxBins sets the N.
 *                      This limit does not affect what is being returned as number of bins. It
 *                      always returns the dictinct value count.
 * @param aggregateFunction function that implements aggregation logic.
 */
case class Histogram(
                      override val column: String,
                      override val binningUdf: Option[UserDefinedFunction] = None,
                      override val maxDetailBins: Integer = Histogram.MaximumAllowedDetailBins,
                      override val where: Option[String] = None,
                      override val computeFrequenciesAsRatio: Boolean = true,
                      override val aggregateFunction: AggregateFunction = Count)
  extends HistogramBase(column, binningUdf, maxDetailBins, where, computeFrequenciesAsRatio, aggregateFunction)

object Histogram {
  val NullFieldReplacement = "NullValue"
  val MaximumAllowedDetailBins = 1000
  val count_function = "count"
  val sum_function = "sum"

  sealed trait AggregateFunction {
    def query(column: String, data: DataFrame): DataFrame

    def total(data: DataFrame): Long

    def aggregateColumn(): Option[String]

    def function(): String
  }

  case object Count extends AggregateFunction {
    override def query(column: String, data: DataFrame): DataFrame = {
      data
        .select(col(column).cast(StringType))
        .na.fill(Histogram.NullFieldReplacement)
        .groupBy(column)
        .count()
        .withColumnRenamed("count", Analyzers.COUNT_COL)
    }

    override def aggregateColumn(): Option[String] = None

    override def function(): String = count_function

    override def total(data: DataFrame): Long = {
      data.count()
    }
  }

  case class Sum(aggColumn: String) extends AggregateFunction {
    override def query(column: String, data: DataFrame): DataFrame = {
      data
        .select(col(column).cast(StringType), col(aggColumn).cast(LongType))
        .na.fill(Histogram.NullFieldReplacement)
        .groupBy(column)
        .sum(aggColumn)
        .withColumnRenamed("count", Analyzers.COUNT_COL)
    }

    override def total(data: DataFrame): Long = {
      data.groupBy().sum(aggColumn).first().getLong(0)
    }

    override def aggregateColumn(): Option[String] = {
      Some(aggColumn)
    }

    override def function(): String = sum_function
  }
}

object OrderByAbsoluteCount extends Ordering[Row] {
  override def compare(x: Row, y: Row): Int = {
    x.getLong(1).compareTo(y.getLong(1))
  }
}
