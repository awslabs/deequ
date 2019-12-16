/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 *
 */

package com.amazon.deequ.profiles

import com.amazon.deequ.analyzers.DataTypeInstances
import com.amazon.deequ.metrics.{BucketDistribution, Distribution}
import com.google.gson.{Gson, GsonBuilder, JsonArray, JsonObject, JsonPrimitive}

/* Profiling results for the columns which will be given to the constraint suggestion engine */
abstract class ColumnProfile {
  def column: String
  def completeness: Double
  def approximateNumDistinctValues: Long
  def dataType: DataTypeInstances.Value
  def isDataTypeInferred: Boolean
  def typeCounts: Map[String, Long]
  def histogram: Option[Distribution]
}

case class StandardColumnProfile(
    column: String,
    completeness: Double,
    approximateNumDistinctValues: Long,
    dataType: DataTypeInstances.Value,
    isDataTypeInferred: Boolean,
    typeCounts: Map[String, Long],
    histogram: Option[Distribution])
  extends ColumnProfile

case class NumericColumnProfile(
    column: String,
    completeness: Double,
    approximateNumDistinctValues: Long,
    dataType: DataTypeInstances.Value,
    isDataTypeInferred: Boolean,
    typeCounts: Map[String, Long],
    histogram: Option[Distribution],
    kll: Option[BucketDistribution],
    mean: Option[Double],
    maximum: Option[Double],
    minimum: Option[Double],
    sum: Option[Double],
    stdDev: Option[Double],
    approxPercentiles: Option[Seq[Double]])
  extends ColumnProfile

case class ColumnProfiles(
    profiles: Map[String, ColumnProfile],
    numRecords: Long)


object ColumnProfiles {

  def toJson(columnProfiles: Seq[ColumnProfile]): String = {

    val json = new JsonObject()

    val columns = new JsonArray()

    columnProfiles.foreach { case profile =>

      val columnProfileJson = new JsonObject()
      columnProfileJson.addProperty("column", profile.column)
      columnProfileJson.addProperty("dataType", profile.dataType.toString)
      columnProfileJson.addProperty("isDataTypeInferred", profile.isDataTypeInferred.toString)

      if (profile.typeCounts.nonEmpty) {
        val typeCountsJson = new JsonObject()
        profile.typeCounts.foreach { case (typeName, count) =>
          typeCountsJson.addProperty(typeName, count.toString)
        }
      }

      columnProfileJson.addProperty("completeness", profile.completeness)
      columnProfileJson.addProperty("approximateNumDistinctValues",
        profile.approximateNumDistinctValues)

      if (profile.histogram.isDefined) {
        val histogram = profile.histogram.get
        val histogramJson = new JsonArray()

        histogram.values.foreach { case (name, distributionValue) =>
          val histogramEntry = new JsonObject()
          histogramEntry.addProperty("value", name)
          histogramEntry.addProperty("count", distributionValue.absolute)
          histogramEntry.addProperty("ratio", distributionValue.ratio)
          histogramJson.add(histogramEntry)
        }

        columnProfileJson.add("histogram", histogramJson)
      }

      profile match {
        case numericColumnProfile: NumericColumnProfile =>
          numericColumnProfile.mean.foreach { mean =>
            columnProfileJson.addProperty("mean", mean)
          }
          numericColumnProfile.maximum.foreach { maximum =>
            columnProfileJson.addProperty("maximum", maximum)
          }
          numericColumnProfile.minimum.foreach { minimum =>
            columnProfileJson.addProperty("minimum", minimum)
          }
          numericColumnProfile.sum.foreach { sum =>
            columnProfileJson.addProperty("sum", sum)
          }
          numericColumnProfile.stdDev.foreach { stdDev =>
            columnProfileJson.addProperty("stdDev", stdDev)
          }

          // KLL Sketch
          if (numericColumnProfile.kll.isDefined) {
            val kllSketch = numericColumnProfile.kll.get
            val kllSketchJson = new JsonObject()

            val tmp = new JsonArray()
            kllSketch.buckets.foreach{bucket =>
              val entry = new JsonObject()
              entry.addProperty("low_value", bucket.lowValue)
              entry.addProperty("high_value", bucket.highValue)
              entry.addProperty("count", bucket.count)
              tmp.add(entry)
            }

            kllSketchJson.add("buckets", tmp)
            val entry = new JsonObject()
            entry.addProperty("c", kllSketch.parameters(0))
            entry.addProperty("k", kllSketch.parameters(1))
            val store = new JsonObject()
            store.add("parameters", entry)

            val gson = new Gson()
            val dataJson = gson.toJson(kllSketch.data)

            store.addProperty("data", dataJson)

            kllSketchJson.add("sketch", store)
            columnProfileJson.add("kll", kllSketchJson)
          }

          val approxPercentilesJson = new JsonArray()
          numericColumnProfile.approxPercentiles.foreach {
            _.foreach { percentile =>
              approxPercentilesJson.add(new JsonPrimitive(percentile))
            }
          }

          columnProfileJson.add("approxPercentiles", approxPercentilesJson)

        case _ =>
      }

      columns.add(columnProfileJson)
    }

    json.add("columns", columns)

    val gson = new GsonBuilder()
      .setPrettyPrinting()
      .create()

    gson.toJson(json)
  }
}
