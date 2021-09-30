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
  JsonDeserializationContext,
  JsonDeserializer,
  JsonElement
}

import java.lang.reflect.Type

object AnalysisResultDeserializer extends JsonDeserializer[AnalysisResult] {

  override def deserialize(
      jsonElement: JsonElement,
      t: Type,
      context: JsonDeserializationContext
  ): AnalysisResult = {

    val jsonObject = jsonElement.getAsJsonObject

    val key: ResultKey =
      context.deserialize(jsonObject.get(RESULT_KEY_FIELD), classOf[ResultKey])
    val analyzerContext: AnalyzerContext = context
      .deserialize(
        jsonObject.get(ANALYZER_CONTEXT_FIELD),
        classOf[AnalyzerContext]
      )

    AnalysisResult(key, analyzerContext)
  }
}
