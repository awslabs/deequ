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

import com.amazon.deequ.metrics.{Distribution, DistributionValue}
import com.google.gson.{
  JsonDeserializationContext,
  JsonDeserializer,
  JsonElement
}

import java.lang.reflect.Type
import scala.collection.JavaConverters._

object DistributionDeserializer extends JsonDeserializer[Distribution] {

  override def deserialize(
      jsonElement: JsonElement,
      t: Type,
      context: JsonDeserializationContext
  ): Distribution = {

    val jsonObject = jsonElement.getAsJsonObject

    val values = jsonObject
      .get("values")
      .getAsJsonObject
      .entrySet
      .asScala
      .map { entry =>
        val distributionValue = entry.getValue.getAsJsonObject
        (
          entry.getKey,
          DistributionValue(
            distributionValue.get("absolute").getAsLong,
            distributionValue.get("ratio").getAsDouble
          )
        )
      }
      .toMap

    Distribution(values, jsonObject.get("numberOfBins").getAsLong)
  }
}
