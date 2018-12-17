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

import com.amazon.deequ.analyzers.runners.AnalyzerContext

/**
  * Common trait for RepositoryIndexes where deequ runs can be stored.
  * Repository provides methods to store AnalysisResults(metrics) and VerificationResults(if any)
  */
trait MetricsRepository {

  /**
    * Saves Analysis results (metrics)
    *
    * @param resultKey       A ResultKey that uniquely identifies a AnalysisResult
    * @param analyzerContext The resulting AnalyzerContext of an Analysis
    */
  def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit

  /**
    * Get a AnalyzerContext saved using exactly the same resultKey if present
    */
  def loadByKey(resultKey: ResultKey): Option[AnalyzerContext]

  /** Get a builder class to construct a loading query to get AnalysisResults */
  def load(): MetricsRepositoryMultipleResultsLoader

}

/**
  * Information that uniquely identifies a AnalysisResult
  *
  * @param dataSetDate A date related to the AnalysisResult
  * @param tags        A map with additional annotations
  */
case class ResultKey(dataSetDate: Long, tags: Map[String, String] = Map.empty)
