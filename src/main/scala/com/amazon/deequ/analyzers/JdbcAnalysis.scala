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

import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzer, JdbcStateLoader, JdbcStatePersister, Table}
import com.amazon.deequ.analyzers.runners.{JdbcAnalysisRunner, JdbcAnalyzerContext}
import com.amazon.deequ.metrics.Metric
import org.apache.spark.storage.StorageLevel

/**
  * Defines a set of analyzers to run on data.
  *
  * @param analyzers
  */
case class JdbcAnalysis(analyzers: Seq[JdbcAnalyzer[_, Metric[_]]] = Seq.empty) {

  def addAnalyzer(analyzer: JdbcAnalyzer[_, Metric[_]]): JdbcAnalysis = {
    JdbcAnalysis(analyzers :+ analyzer)
  }

  def addAnalyzers(otherAnalyzers: Seq[JdbcAnalyzer[_, Metric[_]]]): JdbcAnalysis = {
    JdbcAnalysis(analyzers ++ otherAnalyzers)
  }

  /**
    * Compute the metrics from the analyzers configured in the analyis
    *
    * @param data data on which to operate
    * @param aggregateWith load existing states for the configured analyzers and aggregate them
    *                      (optional)
    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
    * @param storageLevelOfGroupedDataForMultiplePasses caching level for grouped data that must
    *                                                   be accessed multiple times (use
    *                                                   StorageLevel.NONE to completely disable
    *                                                   caching)
    * @return
    */
  @deprecated("Use the AnalysisRunner instead (the onData method there)", "24-09-2019")
  def run(
      data: Table,
      aggregateWith: Option[JdbcStateLoader] = None,
      saveStatesWith: Option[JdbcStatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)
    : JdbcAnalyzerContext = {

    JdbcAnalysisRunner.doAnalysisRun(data, analyzers, aggregateWith = aggregateWith,
      saveStatesWith = saveStatesWith)
  }
}
