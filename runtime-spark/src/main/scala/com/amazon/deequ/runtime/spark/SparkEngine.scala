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

package com.amazon.deequ.runtime.spark

import com.amazon.deequ.ComputedStatistics
import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.profiles.{ColumnProfiler, ColumnProfiles}
import com.amazon.deequ.runtime.{Dataset, Engine, EngineRepositoryOptions}
import com.amazon.deequ.statistics.Statistic
import org.apache.spark.sql.SparkSession
import com.amazon.deequ.analyzers._
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.statistics._

case class SparkEngine(session: SparkSession) extends Engine {

  override def compute(
      data: Dataset,
      statistics: Seq[Statistic],
      engineRepositoryOptions: EngineRepositoryOptions
    ): ComputedStatistics = {

    val analyzers = statistics.map { SparkEngine.matchingOperator }

    val analysisResult = AnalysisRunner.doAnalysisRun(
      data.asInstanceOf[SparkDataset].df,
      analyzers
    )

    val statisticsAndResults = analysisResult.metricMap
      .map { case (analyzer, metric) =>
        SparkEngine.matchingStatistic(analyzer) -> metric
      }
      .toMap[Statistic, Metric[_]]

    ComputedStatistics(statisticsAndResults)
  }

  override def splitTrainTestSets(
      data: Dataset,
      testsetRatio: Option[Double],
      testsetSplitRandomSeed: Option[Long])
    : (Dataset, Option[Dataset]) = {

    val df = data.asInstanceOf[SparkDataset].df

    if (testsetRatio.isDefined) {

      val trainsetRatio = 1.0 - testsetRatio.get
      val Array(trainSplit, testSplit) =
        if (testsetSplitRandomSeed.isDefined) {
          df.randomSplit(Array(trainsetRatio, testsetRatio.get), testsetSplitRandomSeed.get)
        } else {
          df.randomSplit(Array(trainsetRatio, testsetRatio.get))
        }
      (SparkDataset(trainSplit), Some(SparkDataset(testSplit)))
    } else {
      (data, None)
    }
  }

  override def profile(
      dataset: Dataset,
      restrictToColumns: Option[Seq[String]],
      lowCardinalityHistogramThreshold: Int,
      printStatusUpdates: Boolean)
    : ColumnProfiles = {

    val df = dataset.asInstanceOf[SparkDataset].df

    ColumnProfiler.profile(
      df,
      restrictToColumns,
      printStatusUpdates,
      lowCardinalityHistogramThreshold
    )
  }
}

object SparkEngine {

  def matchingOperator(statistic: Statistic): Analyzer[_, Metric[_]] = {

    statistic match {

      case size: Size =>
        SizeOp(size.where)

      case completeness: Completeness =>
        CompletenessOp(completeness.column, completeness.where)

      case compliance: Compliance =>
        ComplianceOp(compliance.instance, compliance.predicate, compliance.where)

      case patternMatch: PatternMatch =>
        PatternMatchOp(patternMatch.column, patternMatch.pattern, patternMatch.where)

      case sum: Sum =>
        SumOp(sum.column, sum.where)

      case mean: Mean =>
        MeanOp(mean.column, mean.where)

      case minimum: Minimum =>
        MinimumOp(minimum.column, minimum.where)

      case maximum: Maximum =>
        MaximumOp(maximum.column, maximum.where)

      case histogram : Histogram =>
        HistogramOp(histogram.column, maxDetailBins = histogram.maxDetailBins)

      case uniqueness: Uniqueness =>
        UniquenessOp(uniqueness.columns)

      //FIXLATER ADD MISSING

      case dataType: DataType =>
        DataTypeOp(dataType.column, dataType.where)

      case approxCountDistinct: ApproxCountDistinct =>
        ApproxCountDistinctOp(approxCountDistinct.column, approxCountDistinct.where)

      case correlation: Correlation =>
        CorrelationOp(correlation.columnA, correlation.columnB, correlation.where)

      case stdDev: StandardDeviation =>
        StandardDeviationOp(stdDev.column, stdDev.where)

      case approxQuantile: ApproxQuantile =>
        ApproxQuantileOp(approxQuantile.column, approxQuantile.quantile)

      case _ =>
        throw new IllegalArgumentException(s"Unable to handle statistic $statistic.")
    }
  }

  def matchingStatistic(analyzer: Analyzer[_, Metric[_]]): Statistic = {

    analyzer match {

      case size: SizeOp =>
        Size(size.where)

      case completeness: CompletenessOp =>
        Completeness(completeness.column, completeness.where)

      case compliance: ComplianceOp =>
        Compliance(compliance.instance, compliance.predicate, compliance.where)

      case patternMatch: PatternMatchOp =>
        PatternMatch(patternMatch.column, patternMatch.pattern, patternMatch.where)

      case sum: SumOp =>
        Sum(sum.column, sum.where)

      case mean: MeanOp =>
        Mean(mean.column, mean.where)

      case minimum: MinimumOp =>
        Minimum(minimum.column, minimum.where)

      case maximum: MaximumOp =>
        Maximum(maximum.column, maximum.where)

      case histogram: HistogramOp =>
        Histogram(histogram.column, maxDetailBins = histogram.maxDetailBins)

      case uniqueness: UniquenessOp =>
        Uniqueness(uniqueness.columns)

      //FIXLATER ADD MISSING

      case dataType: DataTypeOp =>
        DataType(dataType.column, dataType.where)

      case approxCountDistinct: ApproxCountDistinctOp =>
        ApproxCountDistinct(approxCountDistinct.column, approxCountDistinct.where)

      case correlation: CorrelationOp =>
        Correlation(correlation.firstColumn, correlation.secondColumn, correlation.where)

      case stdDev: StandardDeviationOp =>
        StandardDeviation(stdDev.column, stdDev.where)

      case approxQuantile: ApproxQuantileOp =>
        ApproxQuantile(approxQuantile.column, approxQuantile.quantile)

      case _ =>
        throw new IllegalArgumentException(s"Unable to handle operator $analyzer.")
    }
  }

}