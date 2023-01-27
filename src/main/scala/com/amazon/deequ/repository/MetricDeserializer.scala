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

import com.amazon.deequ.metrics.{
  Distribution,
  DoubleMetric,
  Entity,
  HistogramMetric,
  KeyedDoubleMetric,
  Metric
}
import com.amazon.deequ.repository.JsonSerializationConstants.COLUMN_FIELD
import com.google.gson.{
  JsonDeserializationContext,
  JsonDeserializer,
  JsonElement
}

import java.lang.reflect.Type
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._

object MetricDeserializer extends JsonDeserializer[Metric[_]] {

  override def deserialize(
      jsonElement: JsonElement,
      t: Type,
      context: JsonDeserializationContext
  ): Metric[_] = {

    val jsonObject = jsonElement.getAsJsonObject

    val metric = jsonObject.get("metricName").getAsString match {

      case "DoubleMetric" =>
        DoubleMetric(
          Entity.withName(jsonObject.get("entity").getAsString),
          jsonObject.get("name").getAsString,
          jsonObject.get("instance").getAsString,
          Try(jsonObject.get("value").getAsDouble)
        )

      case "HistogramMetric" =>
        HistogramMetric(
          jsonObject.get(COLUMN_FIELD).getAsString,
          Try(
            context
              .deserialize(jsonObject.get("value"), classOf[Distribution])
              .asInstanceOf[Distribution]
          )
        )

      case "KeyedDoubleMetric" =>
        val entity = Entity.withName(jsonObject.get("entity").getAsString)
        val name = jsonObject.get("name").getAsString
        val instance = jsonObject.get("instance").getAsString
        if (jsonObject.has("value")) {
          val entries = jsonObject.get("value").getAsJsonObject
          val values = entries
            .entrySet()
            .map { entry =>
              entry.getKey -> entry.getValue.getAsDouble
            }
            .toMap

          KeyedDoubleMetric(entity, name, instance, Success(values))
        } else {
          // TODO is it ok to have null here?
          KeyedDoubleMetric(entity, name, instance, Failure(null))
        }

      case metricName =>
        throw new IllegalArgumentException(
          s"Unable to deserialize analyzer $metricName."
        )
    }

    metric.asInstanceOf[Metric[_]]
  }
}
