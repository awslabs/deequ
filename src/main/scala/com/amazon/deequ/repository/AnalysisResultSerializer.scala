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

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.JsonSerializationConstants.{
  ANALYZER_CONTEXT_FIELD,
  RESULT_KEY_FIELD
}
import com.google.gson.{
  JsonElement,
  JsonObject,
  JsonSerializationContext,
  JsonSerializer
}

import java.lang.reflect.Type

object AnalysisResultSerializer extends JsonSerializer[AnalysisResult] {

  override def serialize(
      analysisResult: AnalysisResult,
      t: Type,
      context: JsonSerializationContext
  ): JsonElement = {

    val result = new JsonObject()

    result.add(
      RESULT_KEY_FIELD,
      context.serialize(analysisResult.resultKey, classOf[ResultKey])
    )

    result.add(
      ANALYZER_CONTEXT_FIELD,
      context.serialize(
        analysisResult.analyzerContext,
        classOf[AnalyzerContext]
      )
    )

    result
  }
}
