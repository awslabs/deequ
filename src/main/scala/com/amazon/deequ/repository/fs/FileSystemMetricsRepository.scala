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

package com.amazon.deequ.repository.fs

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.AnalysisResult
import com.amazon.deequ.repository.AnalysisResultSerde
import com.amazon.deequ.repository.MetricsRepository
import com.amazon.deequ.repository.MetricsRepositoryMultipleResultsLoader
import com.amazon.deequ.repository.ResultKey
import com.google.common.io.Closeables
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.util.UUID.randomUUID


/** A Repository implementation using a file system */
class FileSystemMetricsRepository(session: SparkSession, path: String) extends MetricsRepository {

  /**
    * Saves Analysis results (metrics)
    *
    * @param resultKey       A ResultKey that represents the version of the dataset deequ
    *                        DQ checks were run on.
    * @param analyzerContext The resulting AnalyzerContext of an Analysis
    */
  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {

    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = AnalyzerContext(successfulMetrics)

    val previousAnalysisResults = load().get().filter(_.resultKey != resultKey)

    val serializedResult = AnalysisResultSerde.serialize(
      previousAnalysisResults ++ Seq(AnalysisResult(resultKey, analyzerContextWithSuccessfulValues))
    )

    FileSystemMetricsRepository.writeToFileOnDfs(session, path, {
      val bytes = serializedResult.getBytes(FileSystemMetricsRepository.CHARSET_NAME)
      _.write(bytes)
    })
  }

  /**
    * Get a AnalyzerContext saved using exactly the same resultKey if present
    *
    * @param resultKey       A ResultKey that represents the version of the dataset deequ
    *                        DQ checks were run on.
    */
  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = {
    load().get().find(_.resultKey == resultKey).map(_.analyzerContext)
  }

  /** Get a builder class to construct a loading query to get AnalysisResults */
  override def load(): MetricsRepositoryMultipleResultsLoader = {
    new FileSystemMetricsRepositoryMultipleResultsLoader(session, path)
  }
}

class FileSystemMetricsRepositoryMultipleResultsLoader(
  session: SparkSession, path: String) extends MetricsRepositoryMultipleResultsLoader {

  private[this] var tagValues: Option[Map[String, String]] = None
  private[this] var forAnalyzers: Option[Seq[Analyzer[_, Metric[_]]]] = None
  private[this] var before: Option[Long] = None
  private[this] var after: Option[Long] = None

  /**
    * Filter out results that don't have specific values for specific tags
    *
    * @param tagValues Map with tag names and the corresponding values to filter for
    */
  def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader = {
    this.tagValues = Option(tagValues)
    this
  }

  /**
    * Choose all metrics that you want to load
    *
    * @param analyzers A sequence of analyers who's resulting metrics you want to load
    */
  def forAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]])
    : MetricsRepositoryMultipleResultsLoader = {

    this.forAnalyzers = Option(analyzers)
    this
  }

  /**
    * Only look at AnalysisResults with a result key with a smaller value
    *
    * @param dateTime The maximum dateTime of AnalysisResults to look at
    */
  def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.before = Option(dateTime)
    this
  }

  /**
    * Only look at AnalysisResults with a result key with a greater value
    *
    * @param dateTime The minimum dateTime of AnalysisResults to look at
    */
  def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.after = Option(dateTime)
    this
  }

  /** Get the AnalysisResult */
  def get(): Seq[AnalysisResult] = {

    val allResults = FileSystemMetricsRepository
      .readFromFileOnDfs(session, path, {
        IOUtils.toString(_, FileSystemMetricsRepository.CHARSET_NAME)
      })
      .map { fileContent => AnalysisResultSerde.deserialize(fileContent) }
      .getOrElse(Seq.empty)

    val selection = allResults
      .filter { result => after.isEmpty || after.get <= result.resultKey.dataSetDate }
      .filter { result => before.isEmpty || result.resultKey.dataSetDate <= before.get }
      .filter { result => tagValues.isEmpty ||
        tagValues.get.toSet.subsetOf(result.resultKey.tags.toSet) }

    selection
      .map { analysisResult =>

        val requestedMetrics = analysisResult
          .analyzerContext
          .metricMap
          .filterKeys(analyzer => forAnalyzers.isEmpty || forAnalyzers.get.contains(analyzer))

        val requestedAnalyzerContext = AnalyzerContext(requestedMetrics)

        AnalysisResult(analysisResult.resultKey, requestedAnalyzerContext)
      }
  }
}


object FileSystemMetricsRepository {

  val CHARSET_NAME = "UTF-8"

  def apply(session: SparkSession, path: String): FileSystemMetricsRepository = {
    new FileSystemMetricsRepository(session, path)
  }

  /* Helper function to write to a binary file on S3 */
  private[fs] def writeToFileOnDfs(
      session: SparkSession,
      path: String,
      writeFunc: BufferedOutputStream => Unit)
    : Unit = {

    // Create, rename and delete are atomic operations, at least should be according to spec
    // Create temp file
    val uuid = randomUUID().toString
    val (fs, qualifiedPath) = asQualifiedPath(session, path)
    val tempQualifiedPath = new Path(s"${qualifiedPath.getParent.toUri}/$uuid.json")

    val fsDataOutputStream = fs.create(tempQualifiedPath, false)

    // Cannot just use fsDataOutputStream.writeUTF because that only works for strings up to a
    // length of 64KB
    val bufferedOutputStream = new BufferedOutputStream(fsDataOutputStream)

    try {
      writeFunc(bufferedOutputStream)
    } finally {
      Closeables.close(bufferedOutputStream, false)
    }
    // Delete first file
    if (fs.exists(qualifiedPath)) {
      fs.delete(qualifiedPath, false)
    }
    // Rename temp file
    fs.rename(tempQualifiedPath, qualifiedPath)
  }

  /* Helper function to read from a binary file on S3 */
  private[fs] def readFromFileOnDfs[T](session: SparkSession, path: String,
    readFunc: BufferedInputStream => T): Option[T] = {

    val (fs, qualifiedPath) = asQualifiedPath(session, path)
    if (fs.isFile(qualifiedPath)) {
      val fsDataInputStream = fs.open(qualifiedPath)

      val bufferedInputStream = new BufferedInputStream(fsDataInputStream)

      try {
        Option(readFunc(bufferedInputStream))
      } finally {
        Closeables.close(bufferedInputStream, false)
      }
    } else {
      None
    }
  }

  /* Make sure we write to the correct filesystem, as EMR clusters also have an internal HDFS */
  private[fs] def asQualifiedPath(session: SparkSession, path: String): (FileSystem, Path) = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(session.sparkContext.hadoopConfiguration)
    val qualifiedPath = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    (fs, qualifiedPath)
  }
}
