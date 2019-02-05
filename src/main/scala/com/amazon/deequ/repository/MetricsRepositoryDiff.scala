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

import java.io.{BufferedWriter, File, FileWriter}

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalysisRunnerRepositoryOptions, AnalyzerContext}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class MetricsRepositoryDiff (session: SparkSession,
                             path: String,
                             saveAllRepositories: Boolean = false) {

  private def jsonUnion(jsonOne: String, jsonTwo: String): String = {

    val objectOne: Seq[AnalysisResult] = AnalysisResultSerde.deserialize(jsonOne)
    val objectTwo: Seq[AnalysisResult] = AnalysisResultSerde.deserialize(jsonTwo)

    val unioned = objectTwo ++ objectOne

    AnalysisResultSerde.serialize(unioned)
  }

  def createAndSaveRepositoryAsJson(data: DataFrame,
                                    analyzers: Seq[Analyzer[_, Metric[_]]]): String = {
    val timestamp = System.currentTimeMillis()
    val metricsFile = new File(path, timestamp.toString + "metrics.json")

    val repository: MetricsRepository =
      FileSystemMetricsRepository(session, metricsFile.getAbsolutePath)

    val resultKey = ResultKey(timestamp, Map("tag" -> "data"))

    val analysisResults = AnalysisRunner.doAnalysisRun(
      data,
      analyzers,
      None,
      None,
      StorageLevel.MEMORY_AND_DISK,
      AnalysisRunnerRepositoryOptions(Some(repository)))

    val currentValueForKey = repository.loadByKey(resultKey)

    // AnalyzerContext entries on the right side of ++ will overwrite the ones on the left
    // if there are two different metric results for the same analyzer
    val valueToSave = currentValueForKey.getOrElse(AnalyzerContext.empty) ++ analysisResults

    if(saveAllRepositories) {
      repository.save(Some(resultKey).get, valueToSave)
    }

    val successfulMetrics = valueToSave.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = AnalyzerContext(successfulMetrics)

    AnalysisResultSerde.serialize(
      Seq(AnalysisResult(Some(resultKey).get, analyzerContextWithSuccessfulValues))
    )
  }

  def aggregateMultipleMetricsRepositories(datasets: Seq[DataFrame],
                                           analyzers: Seq[Analyzer[_, Metric[_]]],
                                           name: String = "multimetricsrepo.json"): Unit = {
    var json: String = ""
    val multiRepoFile = new File(path, name)

    datasets.foreach { data =>
      val tempJson = createAndSaveRepositoryAsJson(data, analyzers)
      if (json == "") {
        json = tempJson
      }
      else {
        json = jsonUnion(json, tempJson)
      }
    }
    val bw = new BufferedWriter(new FileWriter(multiRepoFile))
    bw.write(json)
    bw.close()
  }
}
