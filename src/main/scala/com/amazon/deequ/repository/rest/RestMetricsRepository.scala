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

package com.amazon.deequ.repository.rest

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository._
import com.amazonaws.http.{AmazonHttpClient, DefaultErrorResponseHandler,
  ExecutionContext, HttpResponse, HttpResponseHandler}
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.{AmazonClientException, ClientConfiguration, Request}
import com.google.common.collect.ImmutableList
import com.google.common.io.Closeables
import org.apache.commons.io.IOUtils

import java.io.{BufferedInputStream, ByteArrayInputStream}


/** A simple Repository implementation using AmazonHttpClient to read and write to API
 *
 * readRequest: an endpoint request that read all of the metrics generated so far
 * writeRequest: an endpoint request that write analyzer metrics
 * */
class RestMetricsRepository(readRequest: Request[Void], writeRequest: Request[Void])
  extends MetricsRepository {
  /**
   * Other implementation of this RestApiHelper can be used,
   * by extending RestApiHelper, and call setApiHelper
   * */
  var apiHelper: RestApiHelper = new RestApiHelperImp()
  /**
   * Saves Analysis results (metrics)
   *
   * @param resultKey       A ResultKey that uniquely identifies a AnalysisResult
   * @param analyzerContext The resulting AnalyzerContext of an Analysis
   */
  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val successfulMetrics = analyzerContext.metricMap.filter {
      case (_, metric) => metric.value.isSuccess
    }
    val analyzerContextWithSuccessfulValues = AnalyzerContext(successfulMetrics)
    val serializedResult = AnalysisResultSerde.serialize(
      Seq(AnalysisResult(resultKey, analyzerContextWithSuccessfulValues))
    )

    writeRequest.setContent(new ByteArrayInputStream(serializedResult.getBytes("UTF-8")))

    apiHelper.writeHttpRequest(writeRequest)
  }

  /**
   * Get a AnalyzerContext saved using exactly the same resultKey if present
   */
  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = {
    load().get().find(_.resultKey == resultKey).map(_.analyzerContext)
  }

  /** Get a builder class to construct a loading query to get AnalysisResults */
  override def load(): MetricsRepositoryMultipleResultsLoader = {
    new RestMetricsRepositoryMultipleResultsLoader(apiHelper, readRequest)
  }

  /** Set different implementation of RestApiHelper, instead of the default AmazonHttpClient */
  def setApiHelper(apiHelper: RestApiHelper): Unit = {
    this.apiHelper = apiHelper
  }
}

class RestMetricsRepositoryMultipleResultsLoader(apiHelper: RestApiHelper,
                                                 readRequest: Request[Void])
  extends MetricsRepositoryMultipleResultsLoader {

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
    val contentString = apiHelper.readHttpRequest(readRequest, {
      IOUtils.toString(_, "UTF-8")
    })

    val allResults = contentString
      .map { text => AnalysisResultSerde.deserialize(text) }
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

trait RestApiHelper {
  def writeHttpRequest(writeRequest: Request[Void]): Unit
  def readHttpRequest[T](readRequest: Request[Void], readFunc: BufferedInputStream => T): Option[T]
}

class RestApiHelperImp extends RestApiHelper {
  private val httpClient = new AmazonHttpClient(new ClientConfiguration()
    .withRetryPolicy(PredefinedRetryPolicies.DEFAULT))

  override def writeHttpRequest(writeRequest: Request[Void]): Unit = {
    httpClient
      .requestExecutionBuilder
      .executionContext(new ExecutionContext(true))
      .request(writeRequest)
      .errorResponseHandler(new HttpResponseHandler[AmazonClientException] {
        override def handle(response: HttpResponse): AmazonClientException = {
          throw new AmazonClientException(s"ERROR writing to endpoint: " +
            s"${writeRequest.getEndpoint}. Code: ${response.getStatusCode}")
        }
        override def needsConnectionLeftOpen(): Boolean = false
      })
      .execute(new HttpResponseHandler[Unit] {
        override def handle(response: HttpResponse): Unit = {
          if (response.getStatusCode != 200) {
            throw new AmazonClientException(s"ERROR writing to endpoint: " +
              s"${writeRequest.getEndpoint}. Code: ${response.getStatusCode}")
          }
        }
        override def needsConnectionLeftOpen(): Boolean = false
      })
      .getAwsResponse
  }

  override def readHttpRequest[T](readRequest: Request[Void],
                                  readFunc: BufferedInputStream => T): Option[T] = {
    httpClient
      .requestExecutionBuilder
      .executionContext(new ExecutionContext(true))
      .request(readRequest)
      .errorResponseHandler(new DefaultErrorResponseHandler(ImmutableList.of()))
      .execute(new HttpResponseHandler[Option[T]] {
        override def handle(response: HttpResponse): Option[T] = {
          if (response.getStatusCode == 200) {
            val bufferedInputStream = new BufferedInputStream(response.getContent)
            try {
              Option(readFunc(bufferedInputStream))
            } finally {
              Closeables.close(bufferedInputStream, false)
            }
          } else {
            None
          }
        }
        override def needsConnectionLeftOpen(): Boolean = false
      }).getAwsResponse
  }
}
