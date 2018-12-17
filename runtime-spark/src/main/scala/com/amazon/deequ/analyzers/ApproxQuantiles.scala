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

import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import com.amazon.deequ.analyzers.runners.{IllegalAnalyzerParameterException, MetricCalculationException}
import com.amazon.deequ.metrics.{Entity, KeyedDoubleMetric}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DeequFunctions, Row}
import scala.util.{Failure, Success}

/**
  * Approximate quantile analyzer. The allowed relative error compared to the exact quantile can be
  * configured with `relativeError` parameter. A `relativeError` = 0.0 would yield the exact
  * quantile while increasing the computational load.
  *
  * @param column Column in DataFrame for which the approximate quantile is analyzed.
  * @param quantiles Computed Quantiles. Each entry must be in the interval [0, 1], where 0.5 would
  *                  be the median.
  * @param relativeError Relative target precision to achieve in the quantile computation.
  *                      Must be in the interval [0, 1].
  */
case class ApproxQuantiles(column: String, quantiles: Seq[Double], relativeError: Double = 0.01)
  extends ScanShareableAnalyzer[ApproxQuantileState, KeyedDoubleMetric] {

  val PARAM_CHECKS: StructType => Unit = { _ =>
    quantiles.foreach { quantile =>
      if (quantile < 0.0 || quantile > 1.0) {
        throw new IllegalAnalyzerParameterException(MetricCalculationException
          .getApproxQuantileIllegalParamMessage(quantile))
      }
    }
    if (relativeError < 0.0 || relativeError > 1.0) {
      throw new IllegalAnalyzerParameterException(MetricCalculationException
        .getApproxQuantileIllegalErrorParamMessage(relativeError))
    }
  }

  override private[deequ] def aggregationFunctions() = {
    DeequFunctions.stateful_approx_quantile(col(column), relativeError) :: Nil
  }

  override private[deequ] def fromAggregationResult(
      result: Row,
      offset: Int)
    : Option[ApproxQuantileState] = {

    if (result.isNullAt(offset)) {
      None
    } else {

      val percentileDigest = ApproximatePercentile.serializer.deserialize(
        result.getAs[Array[Byte]](offset))

      Some(ApproxQuantileState(percentileDigest))
    }
  }

  override def computeMetricFrom(state: Option[ApproxQuantileState]): KeyedDoubleMetric = {

    state match {
      case Some(theState) =>
        val digest = theState.percentileDigest
        val computedQuantiles = digest.getPercentiles(quantiles.toArray)

        val results = quantiles.zip(computedQuantiles)
          .map { case (quantile, result) => quantile.toString -> result }
          .toMap

        KeyedDoubleMetric(Entity.Column, "ApproxQuantiles", column, Success(results))

      case _ =>
        toFailureMetric(Analyzers.emptyStateException(this))
    }
  }

  override def toFailureMetric(exception: Exception): KeyedDoubleMetric = {
    KeyedDoubleMetric(Entity.Column, "ApproxQuantiles", column, Failure(
      MetricCalculationException.wrapIfNecessary(exception)))
  }

  override def preconditions: Seq[StructType => Unit] = {
    PARAM_CHECKS :: hasColumn(column) :: isNumeric(column) :: Nil
  }
}
