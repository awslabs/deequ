/** Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
  */

package com.amazon.deequ.repository

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics._
import com.google.gson._
import com.google.gson.reflect.TypeToken

import java.util.{ArrayList => JArrayList, List => JList}
import scala.collection.JavaConverters._
import scala.collection._

object AnalysisResultSerde {

  def serialize(analysisResults: Seq[AnalysisResult]): String = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[ResultKey], ResultKeySerializer)
      .registerTypeAdapter(classOf[AnalysisResult], AnalysisResultSerializer)
      .registerTypeAdapter(classOf[AnalyzerContext], AnalyzerContextSerializer)
      .registerTypeAdapter(
        classOf[Analyzer[State[_], Metric[_]]],
        AnalyzerSerializer
      )
      .registerTypeAdapter(classOf[Metric[_]], MetricSerializer)
      .registerTypeAdapter(classOf[Distribution], DistributionSerializer)
      .setPrettyPrinting()
      .create

    gson.toJson(
      analysisResults.asJava,
      new TypeToken[JList[AnalysisResult]]() {}.getType
    )
  }

  def deserialize(analysisResults: String): Seq[AnalysisResult] = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[ResultKey], ResultKeyDeserializer)
      .registerTypeAdapter(classOf[AnalysisResult], AnalysisResultDeserializer)
      .registerTypeAdapter(
        classOf[AnalyzerContext],
        AnalyzerContextDeserializer
      )
      .registerTypeAdapter(
        classOf[Analyzer[State[_], Metric[_]]],
        AnalyzerDeserializer
      )
      .registerTypeAdapter(classOf[Metric[_]], MetricDeserializer)
      .registerTypeAdapter(classOf[Distribution], DistributionDeserializer)
      .create

    gson
      .fromJson(
        analysisResults,
        new TypeToken[JList[AnalysisResult]]() {}.getType
      )
      .asInstanceOf[JArrayList[AnalysisResult]]
      .asScala
  }
}
