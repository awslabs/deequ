/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class CustomSqlState(stateOrError: Either[Double, String]) extends DoubleValuedState[CustomSqlState] {
  lazy val state = stateOrError.left.get
  lazy val error = stateOrError.right.get

  override def sum(other: CustomSqlState): CustomSqlState = {
    CustomSqlState(Left(state + other.state))
  }

  override def metricValue(): Double = state
}

case class CustomSql(expression: String, disambiguator: String = "*") extends Analyzer[CustomSqlState, DoubleMetric] {
  /**
   * Compute the state (sufficient statistics) from the data
   *
   * @param data data frame
   * @return
   */
  override def computeStateFrom(data: DataFrame, filterCondition: Option[String] = None): Option[CustomSqlState] = {

    Try {
      data.sqlContext.sql(expression)
    } match {
      case Failure(e) => Some(CustomSqlState(Right(e.getMessage)))
      case Success(dfSql) =>
        val cols = dfSql.columns.toSeq
        cols match {
          case Seq(resultCol) =>
            val dfSqlCast = dfSql.withColumn(resultCol, col(resultCol).cast(DoubleType))
            val results: Seq[Row] = dfSqlCast.collect()
            if (results.size != 1) {
              Some(CustomSqlState(Right("Custom SQL did not return exactly 1 row")))
            } else {
              Some(CustomSqlState(Left(results.head.get(0).asInstanceOf[Double])))
            }
          case _ => Some(CustomSqlState(Right("Custom SQL did not return exactly 1 column")))
        }
    }
  }

  /**
   * Compute the metric from the state (sufficient statistics)
   *
   * @param state wrapper holding a state of type S (required due to typing issues...)
   * @return
   */
  override def computeMetricFrom(state: Option[CustomSqlState]): DoubleMetric = {
    state match {
      // The returned state may
      case Some(theState) => theState.stateOrError match {
        case Left(value) => DoubleMetric(Entity.Dataset, "CustomSQL", disambiguator,
          Success(value))
        case Right(error) => DoubleMetric(Entity.Dataset, "CustomSQL", disambiguator,
          Failure(new RuntimeException(error)))
      }
      case None =>
        DoubleMetric(Entity.Dataset, "CustomSQL", disambiguator,
          Failure(new RuntimeException("CustomSql Failed To Run")))
    }
  }

  override private[deequ] def toFailureMetric(failure: Exception) = {
    DoubleMetric(Entity.Dataset, "CustomSQL", disambiguator,
      Failure(new RuntimeException("CustomSql Failed To Run")))
  }
}
