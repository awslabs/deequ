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
  JsonArray,
  JsonElement,
  JsonObject,
  JsonSerializationContext,
  JsonSerializer
}

import java.lang.reflect.Type

object AnalyzerContextSerializer extends JsonSerializer[AnalyzerContext] {

  override def serialize(
      analyzerContext: AnalyzerContext,
      t: Type,
      context: JsonSerializationContext
  ): JsonElement = {

    val result = new JsonObject()

    val metricMap = new JsonArray()

    analyzerContext.metricMap.foreach { case (analyzer, metric) =>
      val entry = new JsonObject()

      entry.add(
        ANALYZER_FIELD,
        context.serialize(analyzer, classOf[Analyzer[State[_], Metric[_]]])
      )
      entry.add(METRIC_FIELD, context.serialize(metric, classOf[Metric[_]]))

      metricMap.add(entry)
    }

    result.add(METRIC_MAP_FIELD, metricMap)

    result
  }
}
