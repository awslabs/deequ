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

package com.amazon.deequ.repository

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.metrics.Metric
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

trait MetricsRepositoryMultipleResultsLoader {

  /**
    * Filter out results that don't have specific values for specific tags
    *
    * @param tagValues Map with tag names and the corresponding values to filter for
    */
  def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader

  /**
    * Choose all metrics that you want to load
    *
    * @param analyzers A sequence of analyers who's resulting metrics you want to load
    */
  def forAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]]): MetricsRepositoryMultipleResultsLoader

  /**
    * Convenience method to only look at AnalysisResults with a history key with a greater value
    *
    * @param dateTime A Long to get only AnalysisResults that happened after
    */
  def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader

  /**
    * Convenience method to only look at AnalysisResults with a history key with a smaller value
    *
    * @param dateTime Only AnalysisResults that happened before
    */
  def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader

  /**
    * Get the AnalysisResult
    */
  def get(): Seq[AnalysisResult]

  /**
    * Get the AnalysisResult as DataFrame
    */
  def getSuccessMetricsAsDataFrame(sparkSession: SparkSession, withTags: Seq[String] = Seq.empty)
  : DataFrame = {
    val analysisResults = get()

    if (analysisResults.isEmpty) {
      // Return an empty DataFrame that still contains the usual columns
      AnalysisResult.getSuccessMetricsAsDataFrame(sparkSession,
        AnalysisResult(ResultKey(0, Map.empty), AnalyzerContext(Map.empty)))
    } else {
      analysisResults
        .map { result =>
          AnalysisResult.getSuccessMetricsAsDataFrame(sparkSession, result, withTags = withTags)
        }
        .reduce(MetricsRepositoryMultipleResultsLoader.dataFrameUnion)
    }
  }

  /** Get the AnalysisResult as DataFrame */
  def getSuccessMetricsAsJson(withTags: Seq[String] = Seq.empty): String = {

    val analysisResults = get()

    if (analysisResults.isEmpty) {
      // Handle this case exactly like directly calling the method on AnalysisResult
      AnalysisResult.getSuccessMetricsAsJson(
        AnalysisResult(ResultKey(0, Map.empty), AnalyzerContext.empty))
    } else {
      analysisResults
        .map { result => AnalysisResult.getSuccessMetricsAsJson(result, withTags = withTags) }
        .reduce(MetricsRepositoryMultipleResultsLoader.jsonUnion)
    }
  }
}

private[repository] object MetricsRepositoryMultipleResultsLoader {

  def jsonUnion(jsonOne: String, jsonTwo: String): String = {

    val objectOne: Seq[Map[String, Any]] = SimpleResultSerde.deserialize(jsonOne)
    val objectTwo: Seq[Map[String, Any]] = SimpleResultSerde.deserialize(jsonTwo)

    val columnsTotal = objectOne.headOption.getOrElse(Map.empty).keySet ++
      objectTwo.headOption.getOrElse(Map.empty).keySet

    val unioned = (objectTwo ++ objectOne).map { map =>

      var columnsToAdd = Map.empty[String, Any]

      columnsTotal.diff(map.keySet).foreach { missingColumn =>
        columnsToAdd = columnsToAdd + (missingColumn -> null)
      }

      map ++ columnsToAdd
    }

    SimpleResultSerde.serialize(unioned)
  }

  def dataFrameUnion(dataFrameOne: DataFrame, dataFrameTwo: DataFrame): DataFrame = {

    val columnsOne = dataFrameOne.columns.toSeq
    val columnsTwo = dataFrameTwo.columns.toSeq
    val columnsTotal = (columnsOne ++ columnsTwo).distinct

    dataFrameOne
      .select(withAllColumns(columnsOne, columnsTotal): _*)
      .union(dataFrameTwo.select(withAllColumns(columnsTwo, columnsTotal): _*))
  }

  def withAllColumns(myCols: Seq[String], allCols: Seq[String]): List[Column] = {
    allCols.toList.map {
      case colName if myCols.contains(colName) => col(colName)
      case colName => lit(null).as(colName)
    }
  }
}
