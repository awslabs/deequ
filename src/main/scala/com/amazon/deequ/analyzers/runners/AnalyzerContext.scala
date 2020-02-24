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

package com.amazon.deequ.analyzers.runners

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.metrics.{DoubleMetric, Metric}
import com.amazon.deequ.repository.SimpleResultSerde
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * The result returned from AnalysisRunner and Analysis
  *
  * @param metricMap Analyzers and their Metric results
  */
case class AnalyzerContext(metricMap: Map[Analyzer[_, Metric[_]], Metric[_]]) {

  def allMetrics: Seq[Metric[_]] = {
    metricMap.values.toSeq
  }

  def ++(other: AnalyzerContext): AnalyzerContext = {
    AnalyzerContext(metricMap ++ other.metricMap)
  }

  def metric(analyzer: Analyzer[_, Metric[_]]): Option[Metric[_]] = {
    metricMap.get(analyzer)
  }
}

object AnalyzerContext {

  def empty: AnalyzerContext = AnalyzerContext(Map.empty)

  def successMetricsAsDataFrame(
      sparkSession: SparkSession,
      analyzerContext: AnalyzerContext,
      forAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty)
    : DataFrame = {

    val metricsList = getSimplifiedMetricOutputForSelectedAnalyzers(analyzerContext, forAnalyzers)

    import sparkSession.implicits._

    metricsList.toDF("entity", "instance", "name", "value")
  }

  def successMetricsAsJson(analyzerContext: AnalyzerContext,
    forAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty): String = {

    val metricsList = getSimplifiedMetricOutputForSelectedAnalyzers(analyzerContext, forAnalyzers)

    val result = metricsList.map { simplifiedMetricOutput =>
      Map(
        "entity" -> simplifiedMetricOutput.entity,
        "instance" -> simplifiedMetricOutput.instance,
        "name" -> simplifiedMetricOutput.name,
        "value" -> simplifiedMetricOutput.value
      )
    }

    SimpleResultSerde.serialize(result)
  }

  private[this] def getSimplifiedMetricOutputForSelectedAnalyzers(
      analyzerContext: AnalyzerContext,
      forAnalyzers: Seq[Analyzer[_, Metric[_]]])
    : Seq[SimpleMetricOutput] = {

    analyzerContext.metricMap
      // Get matching analyzers
      .filterKeys(analyzer => forAnalyzers.isEmpty || forAnalyzers.contains(analyzer))
      // Get analyzers with successful results
      .filter({ case (_, metrics) => metrics.value.isSuccess })
      // Get metrics as Double and replace simple name with description
      .flatMap({ case (analyzer, metrics) =>
        metrics.flatten().map(renameMetric(_, describeAnalyzer(analyzer))) })
      // Simplify metrics
      .map(SimpleMetricOutput(_))
      .toSeq
  }

  private[this] def renameMetric(metric: DoubleMetric, newName: String): DoubleMetric = {
    metric.copy(name = newName)
  }

  /**
    * Describe the Analyzer, using the name of the class, including the value of `where` field
    * if exists.
    * It helps us to show more readable success metrics
    * (see https://github.com/awslabs/deequ/issues/177 for details)
    *
    * @param analyzer the Analyzer to be described
    * @return the description of the Analyzer
    */
  private[this] def describeAnalyzer(analyzer: Any): String = {
    val name = analyzer.getClass.getSimpleName

    ReflectionUtils.getFieldValue(analyzer, "where") match {
      case Some(Some(e)) => s"$name (where: $e)"
      case _ => name
    }
  }

  private[this] case class SimpleMetricOutput(
    entity: String,
    instance: String,
    name: String,
    value: Double)

  private[this] object SimpleMetricOutput {

    def apply(doubleMetric: DoubleMetric): SimpleMetricOutput = {
      SimpleMetricOutput(
        doubleMetric.entity.toString,
        doubleMetric.instance,
        doubleMetric.name,
        doubleMetric.value.get
      )
    }
  }
}
