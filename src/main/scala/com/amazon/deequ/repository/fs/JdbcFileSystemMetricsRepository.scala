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


/*

TODO: add support for writing to local filesystem

/** A Repository implementation using a file system */
class JdbcFileSystemMetricsRepository(connection: Connection, path: String)
  extends JdbcMetricsRepository {

  /**
    * Saves Analysis results (metrics)
    *
    * @param resultKey       A ResultKey that represents the version of the dataset deequ
    *                        DQ checks were run on.
    * @param analyzerContext The resulting AnalyzerContext of an Analysis
    */
  override def save(resultKey: ResultKey, analyzerContext: JdbcAnalyzerContext): Unit = {

    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = JdbcAnalyzerContext(successfulMetrics)

    val previousAnalysisResults = load().get().filter(_.resultKey != resultKey)

    val serializedResult = JdbcAnalysisResultSerde.serialize(
      previousAnalysisResults ++
      Seq(JdbcAnalysisResult(resultKey, analyzerContextWithSuccessfulValues))
    )

    JdbcFileSystemMetricsRepository.writeToFileOnDfs(connection, path, {
      val bytes = serializedResult.getBytes(JdbcFileSystemMetricsRepository.CHARSET_NAME)
      _.write(bytes)
    })
  }

  /**
    * Get a AnalyzerContext saved using exactly the same resultKey if present
    *
    * @param resultKey       A ResultKey that represents the version of the dataset deequ
    *                        DQ checks were run on.
    */
  override def loadByKey(resultKey: ResultKey): Option[JdbcAnalyzerContext] = {
    load().get().find(_.resultKey == resultKey).map(_.analyzerContext)
  }

  /** Get a builder class to construct a loading query to get AnalysisResults */
  override def load(): JdbcMetricsRepositoryMultipleResultsLoader = {
    new JdbcFileSystemMetricsRepositoryMultipleResultsLoader(connection, path)
  }
}

class JdbcFileSystemMetricsRepositoryMultipleResultsLoader(
  connection: Connection, path: String) extends JdbcMetricsRepositoryMultipleResultsLoader {

  private[this] var tagValues: Option[Map[String, String]] = None
  private[this] var forAnalyzers: Option[Seq[JdbcAnalyzer[_, Metric[_]]]] = None
  private[this] var before: Option[Long] = None
  private[this] var after: Option[Long] = None

  /**
    * Filter out results that don't have specific values for specific tags
    *
    * @param tagValues Map with tag names and the corresponding values to filter for
    */
  def withTagValues(tagValues: Map[String, String]): JdbcMetricsRepositoryMultipleResultsLoader = {
    this.tagValues = Option(tagValues)
    this
  }

  /**
    * Choose all metrics that you want to load
    *
    * @param analyzers A sequence of analyers who's resulting metrics you want to load
    */
  def forAnalyzers(analyzers: Seq[JdbcAnalyzer[_, Metric[_]]])
    : JdbcMetricsRepositoryMultipleResultsLoader = {

    this.forAnalyzers = Option(analyzers)
    this
  }

  /**
    * Only look at AnalysisResults with a result key with a smaller value
    *
    * @param dateTime The maximum dateTime of AnalysisResults to look at
    */
  def before(dateTime: Long): JdbcMetricsRepositoryMultipleResultsLoader = {
    this.before = Option(dateTime)
    this
  }

  /**
    * Only look at AnalysisResults with a result key with a greater value
    *
    * @param dateTime The minimum dateTime of AnalysisResults to look at
    */
  def after(dateTime: Long): JdbcMetricsRepositoryMultipleResultsLoader = {
    this.after = Option(dateTime)
    this
  }

  /** Get the AnalysisResult */
  def get(): Seq[JdbcAnalysisResult] = {

    val allResults = JdbcFileSystemMetricsRepository
      .readFromFileOnDfs(connection, path, {
        IOUtils.toString(_, JdbcFileSystemMetricsRepository.CHARSET_NAME)
      })
      .map { fileContent => JdbcAnalysisResultSerde.deserialize(fileContent) }
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

        val requestedAnalyzerContext = JdbcAnalyzerContext(requestedMetrics)

        JdbcAnalysisResult(analysisResult.resultKey, requestedAnalyzerContext)
      }
  }
}


object JdbcFileSystemMetricsRepository {

  val CHARSET_NAME = "UTF-8"

  def apply(connection: Connection, path: String): JdbcFileSystemMetricsRepository = {
    new JdbcFileSystemMetricsRepository(connection, path)
  }

  /* Helper function to write to a binary file on S3 */
  private[fs] def writeToFileOnDfs(
      connection: Connection,
      path: String,
      writeFunc: BufferedOutputStream => Unit)
    : Unit = {

    // Create, rename and delete are atomic operations, at least should be according to spec
    // Create temp file
    val uuid = randomUUID().toString
    val (fs, qualifiedPath) = asQualifiedPath(connection, path)
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
  private[fs] def readFromFileOnDfs[T](connection: Connection, path: String,
    readFunc: BufferedInputStream => T): Option[T] = {

    val (fs, qualifiedPath) = asQualifiedPath(connection, path)
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
  private[fs] def asQualifiedPath(connection: Connection, path: String): (FileSystem, Path) = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(connection.sparkContext.hadoopConfiguration)
    val qualifiedPath = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    (fs, qualifiedPath)
  }
}
*/
