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
import com.amazon.deequ.analyzers.Analyzers.conditionalSelection
import com.amazon.deequ.metrics.DoubleMetric
import org.apache.spark.sql.{DeequFunctions, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.PercentileDigest
import org.apache.spark.sql.types.StructType

case class ApproxQuantileState(percentileDigest: PercentileDigest)
  extends State[ApproxQuantileState] {

  override def sum(other: ApproxQuantileState): ApproxQuantileState = {
    percentileDigest.merge(other.percentileDigest)
    ApproxQuantileState(percentileDigest)
  }
}


/**
  * Approximate quantile analyzer. The allowed relative error compared to the exact quantile can be
  * configured with `relativeError` parameter. A `relativeError` = 0.0 would yield the exact
  * quantile while increasing the computational load.
  *
  * @param column Column in DataFrame for which the approximate quantile is analyzed.
  * @param quantile Computed Quantile. Must be in the interval [0, 1], where 0.5 would be the
  *                 median.
  * @param relativeError Relative target precision to achieve in the quantile computation.
  *                      Must be in the interval [0, 1].
  * @param where Additional filter to apply before the analyzer is run.
  */
case class ApproxQuantile(
  column: String,
  quantile: Double,
  relativeError: Double = 0.01,
  where: Option[String] = None)
  extends ScanShareableAnalyzer[ApproxQuantileState, DoubleMetric]
  with FilterableAnalyzer {

  val PARAM_CHECKS: StructType => Unit = { _ =>
    if (quantile < 0.0 || quantile > 1.0) {
      throw new IllegalAnalyzerParameterException(MetricCalculationException
        .getApproxQuantileIllegalParamMessage(quantile))
    }
    if (relativeError < 0.0 || relativeError > 1.0) {
      throw new IllegalAnalyzerParameterException(MetricCalculationException
        .getApproxQuantileIllegalErrorParamMessage(relativeError))
    }
  }

  override private[deequ] def aggregationFunctions() = {
    DeequFunctions
      .stateful_approx_quantile(conditionalSelection(column, where), relativeError) :: Nil
  }

  override private[deequ] def fromAggregationResult(
      result: Row, offset: Int)
    : Option[ApproxQuantileState] = {

    if (result.isNullAt(offset)) {
      None
    } else {
      val digest = ApproximatePercentile.serializer.deserialize(result.getAs[Array[Byte]](offset))

      // Handles the case when all values are NULL
      if (digest.getPercentiles(Array(quantile)).isEmpty) {
        None
      } else {
        Some(ApproxQuantileState(digest))
      }
    }
  }

  override def computeMetricFrom(state: Option[ApproxQuantileState]): DoubleMetric = {

    state match {
      case Some(theState) =>
        val percentile = theState.percentileDigest.getPercentiles(Array(quantile)).head
        Analyzers.metricFromValue(percentile, s"ApproxQuantile-$quantile", column)
      case _ =>
        Analyzers.metricFromEmpty(this, s"ApproxQuantile-$quantile", column)
    }
  }

  override def toFailureMetric(exception: Exception): DoubleMetric = {
    Analyzers.metricFromFailure(exception, s"ApproxQuantile-$quantile", column)
  }

  override def preconditions: Seq[StructType => Unit] = {
    PARAM_CHECKS :: hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}
