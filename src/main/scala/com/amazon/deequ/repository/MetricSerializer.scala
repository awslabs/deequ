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
  HistogramMetric,
  KeyedDoubleMetric,
  Metric
}
import com.amazon.deequ.repository.JsonSerializationConstants.COLUMN_FIELD
import com.google.gson.{
  JsonElement,
  JsonObject,
  JsonSerializationContext,
  JsonSerializer
}

import java.lang.reflect.Type

object MetricSerializer extends JsonSerializer[Metric[_]] {

  override def serialize(
      metric: Metric[_],
      t: Type,
      context: JsonSerializationContext
  ): JsonElement = {

    val result = new JsonObject()

    metric match {

      case _ if metric.value.isFailure =>
        throw new IllegalArgumentException(
          s"Unable to serialize failed metrics."
        )

      case doubleMetric: DoubleMetric =>
        result.addProperty("metricName", "DoubleMetric")
        result.addProperty("entity", doubleMetric.entity.toString)
        result.addProperty("instance", doubleMetric.instance)
        result.addProperty("name", doubleMetric.name)
        result.addProperty(
          "value",
          doubleMetric.value.getOrElse(null).asInstanceOf[Double]
        )

      case histogramMetric: HistogramMetric =>
        result.addProperty("metricName", "HistogramMetric")
        result.addProperty(COLUMN_FIELD, histogramMetric.column)
        result.addProperty(
          "numberOfBins",
          histogramMetric.value.get.numberOfBins
        )
        result.add(
          "value",
          context.serialize(histogramMetric.value.get, classOf[Distribution])
        )

      case keyedDoubleMetric: KeyedDoubleMetric =>
        result.addProperty("metricName", "KeyedDoubleMetric")
        result.addProperty("entity", keyedDoubleMetric.entity.toString)
        result.addProperty("instance", keyedDoubleMetric.instance)
        result.addProperty("name", keyedDoubleMetric.name)

        if (keyedDoubleMetric.value.isSuccess) {
          val values = new JsonObject()
          keyedDoubleMetric.value.get.foreach { case (key, value) =>
            values.addProperty(key, value)
          }
          result.add("value", values)
        }

      case _ =>
        throw new IllegalArgumentException(
          s"Unable to serialize metrics $metric."
        )
    }

    result
  }
}
