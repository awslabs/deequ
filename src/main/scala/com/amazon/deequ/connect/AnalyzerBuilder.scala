/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 */

package com.amazon.deequ.connect

import com.amazon.deequ.analyzers._
import com.amazon.deequ.connect.proto.AnalyzerMessage
import com.amazon.deequ.metrics.Metric

import scala.collection.JavaConverters._
import scala.util.matching.Regex

/**
 * Builds Deequ Analyzer objects from protobuf messages.
 */
object AnalyzerBuilder {

  // Type alias for analyzers with proper bounds
  type DeequAnalyzer = Analyzer[_ <: State[_], Metric[_]]

  /**
   * Build an Analyzer from an AnalyzerMessage protobuf.
   */
  def build(msg: AnalyzerMessage): DeequAnalyzer = {
    val where = if (msg.getWhere.isEmpty) None else Some(msg.getWhere)

    msg.getType match {
      // Basic analyzers
      case "Size" =>
        Size(where)

      case "Completeness" =>
        Completeness(msg.getColumn, where)

      case "Compliance" =>
        // For compliance, the pattern field contains the SQL predicate
        val predicate = if (msg.getPattern.isEmpty) "true" else msg.getPattern
        Compliance(msg.getColumn, predicate, where)

      // Statistical analyzers
      case "Mean" =>
        Mean(msg.getColumn, where)

      case "Sum" =>
        Sum(msg.getColumn, where)

      case "StandardDeviation" =>
        StandardDeviation(msg.getColumn, where)

      case "Minimum" =>
        Minimum(msg.getColumn, where)

      case "Maximum" =>
        Maximum(msg.getColumn, where)

      case "MinLength" =>
        MinLength(msg.getColumn, where)

      case "MaxLength" =>
        MaxLength(msg.getColumn, where)

      // Quantile analyzers
      case "ApproxQuantile" =>
        val quantile = if (msg.getQuantile == 0.0) 0.5 else msg.getQuantile
        val relativeError = if (msg.getRelativeError == 0.0) 0.01 else msg.getRelativeError
        ApproxQuantile(msg.getColumn, quantile, relativeError)

      case "ApproxQuantiles" =>
        val quantiles = if (msg.getColumnsList.isEmpty) {
          Seq(0.25, 0.5, 0.75)
        } else {
          msg.getColumnsList.asScala.map(_.toDouble).toSeq
        }
        val relativeError = if (msg.getRelativeError == 0.0) 0.01 else msg.getRelativeError
        ApproxQuantiles(msg.getColumn, quantiles, relativeError)

      // Uniqueness analyzers
      case "Uniqueness" =>
        val columns = if (msg.getColumnsList.isEmpty) {
          Seq(msg.getColumn)
        } else {
          msg.getColumnsList.asScala.toSeq
        }
        Uniqueness(columns, where)

      case "Distinctness" =>
        val columns = if (msg.getColumnsList.isEmpty) {
          Seq(msg.getColumn)
        } else {
          msg.getColumnsList.asScala.toSeq
        }
        Distinctness(columns, where)

      case "UniqueValueRatio" =>
        val columns = if (msg.getColumnsList.isEmpty) {
          Seq(msg.getColumn)
        } else {
          msg.getColumnsList.asScala.toSeq
        }
        UniqueValueRatio(columns, where)

      case "CountDistinct" =>
        val columns = if (msg.getColumnsList.isEmpty) {
          Seq(msg.getColumn)
        } else {
          msg.getColumnsList.asScala.toSeq
        }
        CountDistinct(columns)

      case "ApproxCountDistinct" =>
        ApproxCountDistinct(msg.getColumn, where)

      // Pattern analyzers
      case "PatternMatch" =>
        val pattern = new Regex(msg.getPattern)
        PatternMatch(msg.getColumn, pattern, where)

      // Histogram
      case "Histogram" =>
        val maxBins = if (msg.getMaxDetailBins == 0) {
          Histogram.MaximumAllowedDetailBins
        } else {
          msg.getMaxDetailBins
        }
        Histogram(msg.getColumn, None, maxBins)

      // Entropy and information theory
      case "Entropy" =>
        Entropy(msg.getColumn, where)

      case "MutualInformation" =>
        val columns = msg.getColumnsList.asScala.toSeq
        if (columns.size >= 2) {
          MutualInformation(columns)
        } else {
          throw new IllegalArgumentException("MutualInformation requires at least 2 columns")
        }

      // Correlation
      case "Correlation" =>
        val columns = msg.getColumnsList.asScala.toSeq
        if (columns.size >= 2) {
          Correlation(columns(0), columns(1), where)
        } else {
          throw new IllegalArgumentException("Correlation requires exactly 2 columns")
        }

      // Data type
      case "DataType" =>
        DataType(msg.getColumn, where)

      // Unknown analyzer type
      case unknownType =>
        throw new IllegalArgumentException(s"Unknown analyzer type: $unknownType")
    }
  }
}
