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

import com.amazon.deequ.metrics.Distribution
import com.google.gson.{
  JsonElement,
  JsonObject,
  JsonSerializationContext,
  JsonSerializer
}

import java.lang.reflect.Type

object DistributionSerializer extends JsonSerializer[Distribution] {

  override def serialize(
      distribution: Distribution,
      t: Type,
      context: JsonSerializationContext
  ): JsonElement = {

    val result = new JsonObject()
    result.addProperty("numberOfBins", distribution.numberOfBins)

    val values = new JsonObject()

    distribution.values.foreach { case (key, distributionValue) =>
      val value = new JsonObject()
      value.addProperty("absolute", distributionValue.absolute)
      value.addProperty("ratio", distributionValue.ratio)
      values.add(key, value)
    }

    result.add("values", values)

    result
  }
}
