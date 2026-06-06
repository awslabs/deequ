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
import com.amazon.deequ.connect.proto.{Analyzer => ProtoAnalyzer}
import com.amazon.deequ.metrics.Metric

import scala.collection.JavaConverters._
import scala.util.matching.Regex

/**
 * Builds Deequ Analyzer objects from the Stage 1 protobuf shape.
 *
 * Each analyzer kind is encoded as a `oneof` arm on `Analyzer`, with one arm
 * per Deequ analyzer constructor. We pattern-match on `getBodyCase`; the
 * Scala compiler enforces exhaustiveness.
 *
 * `where` lives on the `Analyzer` parent and is ignored by analyzers whose
 * underlying Deequ constructors do not accept it (e.g. ApproxQuantile,
 * Histogram, MutualInformation). See ADR-0001.
 */
object AnalyzerBuilder {

  type DeequAnalyzer = Analyzer[_ <: State[_], Metric[_]]

  def build(msg: ProtoAnalyzer): DeequAnalyzer = {
    val where = if (msg.getWhere.isEmpty) None else Some(msg.getWhere)

    import ProtoAnalyzer.BodyCase._
    msg.getBodyCase match {
      // ---- No-parameter ----------------------------------------------------
      case SIZE =>
        Size(where)

      // ---- Single-column analyzers ----------------------------------------
      case COMPLETENESS =>
        Completeness(msg.getCompleteness.getColumn, where)

      case MEAN =>
        Mean(msg.getMean.getColumn, where)

      case SUM =>
        Sum(msg.getSum.getColumn, where)

      case STANDARD_DEVIATION =>
        StandardDeviation(msg.getStandardDeviation.getColumn, where)

      case MINIMUM =>
        Minimum(msg.getMinimum.getColumn, where)

      case MAXIMUM =>
        Maximum(msg.getMaximum.getColumn, where)

      case MIN_LENGTH =>
        MinLength(msg.getMinLength.getColumn, where)

      case MAX_LENGTH =>
        MaxLength(msg.getMaxLength.getColumn, where)

      case APPROX_COUNT_DISTINCT =>
        ApproxCountDistinct(msg.getApproxCountDistinct.getColumn, where)

      case ENTROPY =>
        Entropy(msg.getEntropy.getColumn, where)

      case DATA_TYPE =>
        DataType(msg.getDataType.getColumn, where)

      // ---- Multi-column analyzers -----------------------------------------
      case UNIQUENESS =>
        Uniqueness(msg.getUniqueness.getColumnsList.asScala.toSeq, where)

      case DISTINCTNESS =>
        Distinctness(msg.getDistinctness.getColumnsList.asScala.toSeq, where)

      case UNIQUE_VALUE_RATIO =>
        UniqueValueRatio(msg.getUniqueValueRatio.getColumnsList.asScala.toSeq, where)

      case COUNT_DISTINCT =>
        CountDistinct(msg.getCountDistinct.getColumnsList.asScala.toSeq)

      case MUTUAL_INFORMATION =>
        MutualInformation(msg.getMutualInformation.getColumnsList.asScala.toSeq)

      // ---- Pair-of-columns analyzer ---------------------------------------
      case CORRELATION =>
        val s = msg.getCorrelation
        Correlation(s.getColumnA, s.getColumnB, where)

      // ---- Approx quantile analyzers --------------------------------------
      case APPROX_QUANTILE =>
        val s = msg.getApproxQuantile
        val quantile = if (s.hasQuantile) s.getQuantile else 0.5
        val relativeError = if (s.hasRelativeError) s.getRelativeError else 0.01
        ApproxQuantile(s.getColumn, quantile, relativeError)

      case APPROX_QUANTILES =>
        val s = msg.getApproxQuantiles
        val quantiles =
          if (s.getQuantilesList.isEmpty) Seq(0.25, 0.5, 0.75)
          else s.getQuantilesList.asScala.map(_.toDouble).toSeq
        val relativeError = if (s.hasRelativeError) s.getRelativeError else 0.01
        ApproxQuantiles(s.getColumn, quantiles, relativeError)

      // ---- Specialised analyzers ------------------------------------------
      case HISTOGRAM =>
        val s = msg.getHistogram
        val maxBins =
          if (s.hasMaxDetailBins) s.getMaxDetailBins else Histogram.MaximumAllowedDetailBins
        Histogram(s.getColumn, None, maxBins)

      case COMPLIANCE =>
        val s = msg.getCompliance
        Compliance(s.getInstance, s.getPredicate, where)

      case PATTERN_MATCH =>
        val s = msg.getPatternMatch
        PatternMatch(s.getColumn, new Regex(s.getPattern), where)

      case BODY_NOT_SET =>
        throw new IllegalArgumentException(
          "Analyzer has no body set; client must populate one of the oneof arms")
    }
  }
}
