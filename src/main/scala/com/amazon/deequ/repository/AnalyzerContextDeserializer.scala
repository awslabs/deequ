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

import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.JsonSerializationConstants.{
  ANALYZER_FIELD,
  METRIC_FIELD,
  METRIC_MAP_FIELD
}
import com.google.gson.{
  JsonDeserializationContext,
  JsonDeserializer,
  JsonElement
}

import java.lang.reflect.Type
import scala.collection.Seq
import scala.collection.JavaConverters._

object AnalyzerContextDeserializer extends JsonDeserializer[AnalyzerContext] {

  override def deserialize(
      jsonElement: JsonElement,
      t: Type,
      context: JsonDeserializationContext
  ): AnalyzerContext = {

    val jsonObject = jsonElement.getAsJsonObject

    val metricMap = jsonObject
      .get(METRIC_MAP_FIELD)
      .getAsJsonArray
      .asScala
      .map { entry =>
        val serializedAnalyzer = entry.getAsJsonObject.get(ANALYZER_FIELD)

        val analyzer = context
          .deserialize(
            serializedAnalyzer,
            classOf[Analyzer[State[_], Metric[_]]]
          )
          .asInstanceOf[Analyzer[State[_], Metric[_]]]

        val metric = context
          .deserialize(
            entry.getAsJsonObject.get(METRIC_FIELD),
            classOf[Metric[_]]
          )
          .asInstanceOf[Metric[_]]

        analyzer.asInstanceOf[Analyzer[State[_], Metric[_]]] -> metric
      }
      .asInstanceOf[Seq[(Analyzer[_, Metric[_]], Metric[_])]]
      .toMap

    AnalyzerContext(metricMap)
  }
}
