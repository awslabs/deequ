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
import com.google.gson.{
  JsonDeserializationContext,
  JsonDeserializer,
  JsonElement
}
import com.google.gson.reflect.TypeToken

import java.lang.reflect.Type
import java.util.{HashMap => JHashMap}
import scala.collection.JavaConverters._

object ResultKeyDeserializer extends JsonDeserializer[ResultKey] {

  override def deserialize(
      jsonElement: JsonElement,
      t: Type,
      context: JsonDeserializationContext
  ): ResultKey = {

    val jsonObject = jsonElement.getAsJsonObject

    val date = jsonObject.get(DATASET_DATE_FIELD).getAsLong
    val tags = context
      .deserialize(
        jsonObject.get(TAGS_FIELD),
        new TypeToken[JHashMap[String, String]]() {}.getType
      )
      .asInstanceOf[JHashMap[String, String]]
      .asScala
      .toMap

    ResultKey(date, tags)
  }
}
