/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
package com.amazon.deequ.repository.sparktable

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

class SparkTableMetricsRepository(session: SparkSession, tableName: String) extends MetricsRepository {

  private val SCHEMA = StructType(Array(
    StructField("result_key", StringType),
    StructField("metric_name", StringType),
    StructField("metric_value", StringType),
    StructField("result_timestamp", StringType),
    StructField("serialized_context", StringType)
  ))

  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val serializedContext = AnalysisResultSerde.serialize(Seq(AnalysisResult(resultKey, analyzerContext)))

    val rows = analyzerContext.metricMap.map { case (analyzer, metric) =>
      Row(resultKey.toString, analyzer.toString, metric.value.toString,
        resultKey.dataSetDate.toString, serializedContext)
    }.toSeq

    val metricDF = session.createDataFrame(session.sparkContext.parallelize(rows), SCHEMA)

    metricDF.write
      .mode(SaveMode.Append)
      .saveAsTable(tableName)
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = {
    val df: DataFrame = session.table(tableName)
    val matchingRows = df.filter(col("result_key") === resultKey.toString).collect()

    if (matchingRows.isEmpty) {
      None
    } else {
      val serializedContext = matchingRows(0).getAs[String]("serialized_context")
      val analysisResult = AnalysisResultSerde.deserialize(serializedContext).head
      Some(analysisResult.analyzerContext)
    }
  }

  override def load(): MetricsRepositoryMultipleResultsLoader = {
    SparkTableMetricsRepositoryMultipleResultsLoader(session, tableName)
  }

}


case class SparkTableMetricsRepositoryMultipleResultsLoader(session: SparkSession,
                                                            tableName: String,
                                                            tagValues: Option[Map[String, String]] = None,
                                                            analyzers: Option[Seq[Analyzer[_, Metric[_]]]] = None,
                                                            timeAfter: Option[Long] = None,
                                                            timeBefore: Option[Long] = None
                                                           ) extends MetricsRepositoryMultipleResultsLoader {

  override def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader =
    this.copy(tagValues = Some(tagValues))

  override def forAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]]): MetricsRepositoryMultipleResultsLoader =
    this.copy(analyzers = Some(analyzers))

  override def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader =
    this.copy(timeAfter = Some(dateTime))

  override def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader =
    this.copy(timeBefore = Some(dateTime))

  override def get(): Seq[AnalysisResult] = {
    val initialDF: DataFrame = session.table(tableName)

    initialDF.printSchema()
    val tagValuesFilter: DataFrame => DataFrame = df => {
      tagValues.map { tags =>
        tags.foldLeft(df) { (currentDF, tag) =>
          currentDF.filter(row => {
            val ser = row.getAs[String]("serialized_context")
            AnalysisResultSerde.deserialize(ser).exists(ar => {
              val tags = ar.resultKey.tags
              tags.contains(tag._1) && tags(tag._1) == tag._2
            })
          })
        }
      }.getOrElse(df)
    }

    val specificAnalyzersFilter: DataFrame => DataFrame = df => {
      analyzers.map(analyzers => df.filter(col("metric_name").isin(analyzers.map(_.toString): _*)))
        .getOrElse(df)
    }

    val timeAfterFilter: DataFrame => DataFrame = df => {
      timeAfter.map(time => df.filter(col("result_timestamp") > time.toString)).getOrElse(df)
    }

    val timeBeforeFilter: DataFrame => DataFrame = df => {
      timeBefore.map(time => df.filter(col("result_timestamp") < time.toString)).getOrElse(df)
    }

    val filteredDF = Seq(tagValuesFilter, specificAnalyzersFilter, timeAfterFilter, timeBeforeFilter)
      .foldLeft(initialDF) {
        (df, filter) => filter(df)
      }

    // Convert the final DataFrame to the desired output format
    filteredDF.collect().flatMap(row => {
      val serializedContext = row.getAs[String]("serialized_context")
      AnalysisResultSerde.deserialize(serializedContext)
    }).toSeq
  }


}
