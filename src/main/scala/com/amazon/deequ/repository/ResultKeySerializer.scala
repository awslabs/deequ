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

import com.amazon.deequ.repository.JsonSerializationConstants.{
  DATASET_DATE_FIELD,
  TAGS_FIELD
}
import com.google.gson.reflect.TypeToken
import com.google.gson._

import java.lang.reflect.Type
import java.util.{Map => JMap}
import scala.collection.JavaConverters._

object ResultKeySerializer extends JsonSerializer[ResultKey] {

  override def serialize(
      resultKey: ResultKey,
      t: Type,
      jsonSerializationContext: JsonSerializationContext
  ): JsonElement = {

    val result = new JsonObject()

    result.add(DATASET_DATE_FIELD, new JsonPrimitive(resultKey.dataSetDate))
    result.add(
      TAGS_FIELD,
      jsonSerializationContext.serialize(
        resultKey.tags.asJava,
        new TypeToken[JMap[String, String]]() {}.getType
      )
    )
    result
  }
}
