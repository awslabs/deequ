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

package com.amazon.deequ.serialization.json

import java.lang.reflect.Type
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import com.amazon.deequ.ComputedStatistics
import com.amazon.deequ.metrics.{Distribution, Metric, _}
import com.amazon.deequ.repository._
import com.amazon.deequ.serialization.json.JsonSerializationConstants._
import com.amazon.deequ.statistics._
import com.google.gson._
import com.google.gson.reflect.TypeToken

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection._
import scala.util.{Failure, Success, Try}

private[deequ] object JsonSerializationConstants {

  val STRING_MAP_TYPE: Type = new TypeToken[JList[JMap[String, Any]]]() {}.getType
  val STRING_LIST_TYPE: Type = new TypeToken[JList[String]]() {}.getType

  // We keep the string "analyzer" here for backwards compatibility
  val STATISTICS_FIELD = "analyzer"
  val STATISTICS_NAME_FIELD = "analyzerName"
  val WHERE_FIELD = "where"
  val COLUMN_FIELD = "column"
  val COLUMNS_FIELD = "columns"
  val METRIC_MAP_FIELD = "metricMap"
  val METRIC_FIELD = "metric"
  val DATASET_DATE_FIELD = "dataSetDate"
  val TAGS_FIELD = "tags"
  val RESULT_KEY_FIELD = "resultKey"
  val COMPUTED_STATISTICS_FIELD = "analyzerContext"
}

private[deequ] object SimpleResultSerde {

  def serialize(successData: Seq[Map[String, Any]]): String = {

    val javaSuccessData = successData.map(_.asJava).asJava

    new GsonBuilder().serializeNulls.create
      .toJson(javaSuccessData, STRING_MAP_TYPE)
  }

  def deserialize(json: String): Seq[immutable.Map[String, Any]] = {
    new GsonBuilder().create
      .fromJson(json, STRING_MAP_TYPE)
      .asInstanceOf[JArrayList[JMap[String, String]]]
      .asScala
      .map(map => immutable.Map(map.asScala.toList: _*))
  }
}

private[deequ] object StatisticsResultSerde {

  def serialize(results: Seq[StatisticsResult]): String = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[ResultKey], ResultKeySerializer)
      .registerTypeAdapter(classOf[StatisticsResult], StatisticsResultSerializer)
      .registerTypeAdapter(classOf[ComputedStatistics], ComputedStatisticsSerializer)
      .registerTypeAdapter(classOf[Statistic],  StatisticSerializer)
      .registerTypeAdapter(classOf[Metric[_]], MetricSerializer)
      .registerTypeAdapter(classOf[Distribution], DistributionSerializer)
      .setPrettyPrinting()
      .create

    gson.toJson(results.asJava, new TypeToken[JList[StatisticsResult]]() {}.getType)
  }

  def deserialize(results: String): Seq[StatisticsResult] = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[ResultKey], ResultKeyDeserializer)
      .registerTypeAdapter(classOf[StatisticsResult], StatisticsResultDeserializer)
      .registerTypeAdapter(classOf[ComputedStatistics], ComputedStatisticsDeserializer)
      .registerTypeAdapter(classOf[Statistic], StatisticDeserializer)
      .registerTypeAdapter(classOf[Metric[_]], MetricDeserializer)
      .registerTypeAdapter(classOf[Distribution], DistributionDeserializer)
      .create

    gson.fromJson(results,
      new TypeToken[JList[StatisticsResult]]() {}.getType)
        .asInstanceOf[JArrayList[StatisticsResult]].asScala
  }
}

private[deequ] object ResultKeySerializer extends JsonSerializer[ResultKey] {

  override def serialize(resultKey: ResultKey, t: Type,
    jsonSerializationContext: JsonSerializationContext): JsonElement = {

    val result = new JsonObject()

    result.add(DATASET_DATE_FIELD, new JsonPrimitive(resultKey.dataSetDate))
    result.add(TAGS_FIELD, jsonSerializationContext.serialize(resultKey.tags.asJava,
      new TypeToken[JMap[String, String]]() {}.getType))
    result
  }
}

private[deequ] object ResultKeyDeserializer extends JsonDeserializer[ResultKey] {

  override def deserialize(jsonElement: JsonElement, t: Type,
                           context: JsonDeserializationContext): ResultKey = {

    val jsonObject = jsonElement.getAsJsonObject

    val date = jsonObject.get(DATASET_DATE_FIELD).getAsLong
    val tags = context.deserialize(jsonObject.get(TAGS_FIELD),
      new TypeToken[JHashMap[String, String]]() {}.getType)
        .asInstanceOf[JHashMap[String, String]].asScala.toMap

    ResultKey(date, tags)
  }
}

private[deequ] object StatisticsResultSerializer extends JsonSerializer[StatisticsResult] {

  override def serialize(statisticsResult: StatisticsResult, t: Type,
    context: JsonSerializationContext): JsonElement = {

    val result = new JsonObject()

    result.add(RESULT_KEY_FIELD, context.serialize(statisticsResult.resultKey, classOf[ResultKey]))

    result.add(COMPUTED_STATISTICS_FIELD,
      context.serialize(statisticsResult.computedStatistics, classOf[ComputedStatistics]))

    result
  }
}

private[deequ] object StatisticsResultDeserializer extends JsonDeserializer[StatisticsResult] {

  override def deserialize(jsonElement: JsonElement, t: Type,
    context: JsonDeserializationContext): StatisticsResult = {

    val jsonObject = jsonElement.getAsJsonObject

    val key: ResultKey = context.deserialize(jsonObject.get(RESULT_KEY_FIELD), classOf[ResultKey])
    val statistics: ComputedStatistics = context
      .deserialize(jsonObject.get(COMPUTED_STATISTICS_FIELD), classOf[ComputedStatistics])

    StatisticsResult(key, statistics)
  }
}

private[deequ] object ComputedStatisticsSerializer extends JsonSerializer[ComputedStatistics] {

  override def serialize(statistics: ComputedStatistics, t: Type,
                         context: JsonSerializationContext): JsonElement = {

    val result = new JsonObject()

    val metricMap = new JsonArray()

    statistics.metricMap.foreach { case (statistic, metric) =>
      val entry = new JsonObject()

      entry.add(STATISTICS_FIELD, context.serialize(statistic, classOf[Statistic]))
      entry.add(METRIC_FIELD, context.serialize(metric, classOf[Metric[_]]))

      metricMap.add(entry)
    }

    result.add(METRIC_MAP_FIELD, metricMap)

    result
  }
}

private[deequ] object ComputedStatisticsDeserializer extends JsonDeserializer[ComputedStatistics] {

  override def deserialize(jsonElement: JsonElement, t: Type,
    context: JsonDeserializationContext): ComputedStatistics = {

    val jsonObject = jsonElement.getAsJsonObject

    val metricMap = jsonObject.get(METRIC_MAP_FIELD).getAsJsonArray.asScala
      .map { entry =>

        val serialized = entry.getAsJsonObject.get(STATISTICS_FIELD)

        val statistic = context.deserialize(serialized, classOf[Statistic]).asInstanceOf[Statistic]

        val metric = context.deserialize(entry.getAsJsonObject.get(METRIC_FIELD),
          classOf[Metric[_]]).asInstanceOf[Metric[_]]

        statistic.asInstanceOf[Statistic] -> metric
      }
      .toMap[Statistic, Metric[_]]

    ComputedStatistics(metricMap)
  }
}

private[deequ] object StatisticSerializer extends JsonSerializer[Statistic] {

  override def serialize(statistic: Statistic, t: Type,
    context: JsonSerializationContext): JsonElement = {

    val result = new JsonObject()

    statistic match {
      case size: Size =>
        result.addProperty(STATISTICS_NAME_FIELD, "Size")
        result.addProperty(WHERE_FIELD, size.where.orNull)


      case completeness: Completeness =>
        result.addProperty(STATISTICS_NAME_FIELD, "Completeness")
        result.addProperty(COLUMN_FIELD, completeness.column)
        result.addProperty(WHERE_FIELD, completeness.where.orNull)


      case compliance: Compliance =>
        result.addProperty(STATISTICS_NAME_FIELD, "Compliance")
        result.addProperty(WHERE_FIELD, compliance.where.orNull)
        result.addProperty("instance", compliance.instance)
        result.addProperty("predicate", compliance.predicate)

      case patternMatch: PatternMatch =>
        result.addProperty(STATISTICS_NAME_FIELD, "PatternMatch")
        result.addProperty(COLUMN_FIELD, patternMatch.column)
        result.addProperty(WHERE_FIELD, patternMatch.where.orNull)
        result.addProperty("pattern", patternMatch.pattern.toString())

      case sum: Sum =>
        result.addProperty(STATISTICS_NAME_FIELD, "Sum")
        result.addProperty(COLUMN_FIELD, sum.column)
        result.addProperty(WHERE_FIELD, sum.where.orNull)

      case mean: Mean =>
        result.addProperty(STATISTICS_NAME_FIELD, "Mean")
        result.addProperty(COLUMN_FIELD, mean.column)
        result.addProperty(WHERE_FIELD, mean.where.orNull)

      case minimum: Minimum =>
        result.addProperty(STATISTICS_NAME_FIELD, "Minimum")
        result.addProperty(COLUMN_FIELD, minimum.column)
        result.addProperty(WHERE_FIELD, minimum.where.orNull)

      case maximum: Maximum =>
        result.addProperty(STATISTICS_NAME_FIELD, "Maximum")
        result.addProperty(COLUMN_FIELD, maximum.column)
        result.addProperty(WHERE_FIELD, maximum.where.orNull)

      case countDistinct: CountDistinct =>
        result.addProperty(STATISTICS_NAME_FIELD, "CountDistinct")
        result.add(COLUMNS_FIELD, context.serialize(countDistinct.columns.asJava,
          new TypeToken[JList[String]]() {}.getType))

      case distinctness: Distinctness =>
        result.addProperty(STATISTICS_NAME_FIELD, "Distinctness")
        result.add(COLUMNS_FIELD, context.serialize(distinctness.columns.asJava))

      case entropy: Entropy =>
        result.addProperty(STATISTICS_NAME_FIELD, "Entropy")
        result.addProperty(COLUMN_FIELD, entropy.column)

      case mutualInformation: MutualInformation =>
        result.addProperty(STATISTICS_NAME_FIELD, "MutualInformation")
        result.add(COLUMNS_FIELD, context.serialize(mutualInformation.columns.asJava,
          new TypeToken[JList[String]]() {}.getType))

      case uniqueValueRatio: UniqueValueRatio =>
        result.addProperty(STATISTICS_NAME_FIELD, "UniqueValueRatio")
        result.add(COLUMNS_FIELD, context.serialize(uniqueValueRatio.columns.asJava,
          new TypeToken[JList[String]]() {}.getType))

      case uniqueness: Uniqueness =>
        result.addProperty(STATISTICS_NAME_FIELD, "Uniqueness")
        result.add(COLUMNS_FIELD, context.serialize(uniqueness.columns.asJava,
          new TypeToken[JList[String]]() {}.getType))

      case histogram: Histogram =>
        result.addProperty(STATISTICS_NAME_FIELD, "Histogram")
        result.addProperty(COLUMN_FIELD, histogram.column)
        result.addProperty("maxDetailBins", histogram.maxDetailBins)

      case dataType: DataType =>
        result.addProperty(STATISTICS_NAME_FIELD, "DataType")
        result.addProperty(COLUMN_FIELD, dataType.column)
        result.addProperty(WHERE_FIELD, dataType.where.orNull)

      case approxCountDistinct: ApproxCountDistinct =>
        result.addProperty(STATISTICS_NAME_FIELD, "ApproxCountDistinct")
        result.addProperty(COLUMN_FIELD, approxCountDistinct.column)
        result.addProperty(WHERE_FIELD, approxCountDistinct.where.orNull)

      case correlation: Correlation =>
        result.addProperty(STATISTICS_NAME_FIELD, "Correlation")
        result.addProperty("firstColumn", correlation.firstColumn)
        result.addProperty("secondColumn", correlation.secondColumn)
        result.addProperty(WHERE_FIELD, correlation.where.orNull)

      case standardDeviation: StandardDeviation =>
        result.addProperty(STATISTICS_NAME_FIELD, "StandardDeviation")
        result.addProperty(COLUMN_FIELD, standardDeviation.column)
        result.addProperty(WHERE_FIELD, standardDeviation.where.orNull)

      case approxQuantile: ApproxQuantile =>
        result.addProperty(STATISTICS_NAME_FIELD, "ApproxQuantile")
        result.addProperty(COLUMN_FIELD, approxQuantile.column)
        result.addProperty("quantile", approxQuantile.quantile)

      case approxQuantiles: ApproxQuantiles =>
        result.addProperty(STATISTICS_NAME_FIELD, "ApproxQuantiles")
        result.addProperty(COLUMN_FIELD, approxQuantiles.column)
        result.addProperty("quantiles", approxQuantiles.quantiles.mkString(","))

      case _ =>
        throw new IllegalArgumentException(s"Unable to serialize statistic $statistic.")
    }

    result
  }
}

private[deequ] object StatisticDeserializer extends JsonDeserializer[Statistic] {

  private[this] def getColumnsAsSeq(context: JsonDeserializationContext,
    json: JsonObject): Seq[String] = {

    context.deserialize(json.get(COLUMNS_FIELD), new TypeToken[JList[String]]() {}.getType)
      .asInstanceOf[JArrayList[String]].asScala
  }

  override def deserialize(jsonElement: JsonElement, t: Type,
    context: JsonDeserializationContext): Statistic = {

    val json = jsonElement.getAsJsonObject

    val statistic = json.get(STATISTICS_NAME_FIELD).getAsString match {

      case "Size" =>
        Size(getOptionalWhereParam(json))

      case "Completeness" =>
        Completeness(json.get(COLUMN_FIELD).getAsString, getOptionalWhereParam(json))

      case "Compliance" =>
        Compliance(
          json.get("instance").getAsString,
          json.get("predicate").getAsString,
          getOptionalWhereParam(json))

      case "PatternMatch" =>
        PatternMatch(
          json.get(COLUMN_FIELD).getAsString,
          json.get("pattern").getAsString.r,
          getOptionalWhereParam(json))

      case "Sum" =>
        Sum(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json))

      case "Mean" =>
        Mean(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json))

      case "Minimum" =>
        Minimum(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json))

      case "Maximum" =>
        Maximum(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json))

      case "CountDistinct" =>
        CountDistinct(getColumnsAsSeq(context, json))

      case "Distinctness" =>
        Distinctness(getColumnsAsSeq(context, json))

      case "Entropy" =>
        Entropy(json.get(COLUMN_FIELD).getAsString)

      case "MutualInformation" =>
        MutualInformation(getColumnsAsSeq(context, json))

      case "UniqueValueRatio" =>
        UniqueValueRatio(getColumnsAsSeq(context, json))

      case "Uniqueness" =>
        Uniqueness(getColumnsAsSeq(context, json))

      case "Histogram" =>
        Histogram(
          json.get(COLUMN_FIELD).getAsString,
          json.get("maxDetailBins").getAsInt)

      case "DataType" =>
        DataType(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json))

      case "ApproxCountDistinct" =>
        ApproxCountDistinct(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json))

      case "Correlation" =>
        Correlation(
          json.get("firstColumn").getAsString,
          json.get("secondColumn").getAsString,
          getOptionalWhereParam(json))

      case "StandardDeviation" =>
        StandardDeviation(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json))

      case "ApproxQuantile" =>
        val column = json.get(COLUMN_FIELD).getAsString
        val quantile = json.get("quantile").getAsDouble
        ApproxQuantile(column, quantile)

      case "ApproxQuantiles" =>
        val column = json.get(COLUMN_FIELD).getAsString
        val quantiles = json.get("quantiles").getAsString.split(",").map { _.toDouble }
        ApproxQuantiles(column, quantiles)

      case name =>
        throw new IllegalArgumentException(s"Unable to deserialize statistic $name.")
    }

    statistic.asInstanceOf[Statistic]
  }

  private[this] def getOptionalWhereParam(jsonObject: JsonObject): Option[String] = {
    if (jsonObject.has(WHERE_FIELD)) {
      Option(jsonObject.get(WHERE_FIELD).getAsString)
    } else {
      None
    }
  }
}

private[deequ] object MetricSerializer extends JsonSerializer[Metric[_]] {

  override def serialize(metric: Metric[_], t: Type,
    context: JsonSerializationContext): JsonElement = {

    val result = new JsonObject()

    metric match {

      case _ if metric.value.isFailure =>
        throw new IllegalArgumentException(s"Unable to serialize failed metrics.")

      case doubleMetric: DoubleMetric =>
        result.addProperty("metricName", "DoubleMetric")
        result.addProperty("entity", doubleMetric.entity.toString)
        result.addProperty("instance", doubleMetric.instance)
        result.addProperty("name", doubleMetric.name)
        result.addProperty("value", doubleMetric.value.getOrElse(null).asInstanceOf[Double])

      case histogramMetric: HistogramMetric =>
        result.addProperty("metricName", "HistogramMetric")
        result.addProperty(COLUMN_FIELD, histogramMetric.column)
        result.addProperty("numberOfBins", histogramMetric.value.get.numberOfBins)
        result.add("value", context.serialize(histogramMetric.value.get,
          classOf[Distribution]))

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
        throw new IllegalArgumentException(s"Unable to serialize metrics $metric.")
    }

    result
  }
}

private[deequ] object MetricDeserializer extends JsonDeserializer[Metric[_]] {

  override def deserialize(jsonElement: JsonElement, t: Type,
    context: JsonDeserializationContext): Metric[_] = {

    val jsonObject = jsonElement.getAsJsonObject

    val metric = jsonObject.get("metricName").getAsString match {

      case "DoubleMetric" =>
        DoubleMetric(
          Entity.withName(jsonObject.get("entity").getAsString),
          jsonObject.get("name").getAsString,
          jsonObject.get("instance").getAsString,
          Try(jsonObject.get("value").getAsDouble))

      case "HistogramMetric" =>
        HistogramMetric(
          jsonObject.get(COLUMN_FIELD).getAsString,
          Try(context.deserialize(jsonObject.get("value"),
            classOf[Distribution]).asInstanceOf[Distribution]))

      case "KeyedDoubleMetric" =>
        val entity = Entity.withName(jsonObject.get("entity").getAsString)
        val name = jsonObject.get("name").getAsString
        val instance = jsonObject.get("instance").getAsString
        if (jsonObject.has("value")) {
          val entries = jsonObject.get("value").getAsJsonObject
          val values = entries.entrySet().map { entry =>
            entry.getKey -> entry.getValue.getAsDouble
          }
          .toMap

          KeyedDoubleMetric(entity, name, instance, Success(values))
        } else {
          // TODO is it ok to have null here?
          KeyedDoubleMetric(entity, name, instance, Failure(null))
        }


      case metricName =>
        throw new IllegalArgumentException(s"Unable to deserialize metric $metricName.")
    }

    metric.asInstanceOf[Metric[_]]
  }
}

private[deequ] object DistributionSerializer extends JsonSerializer[Distribution]{

  override def serialize(distribution: Distribution, t: Type,
    context: JsonSerializationContext): JsonElement = {

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

private[deequ] object DistributionDeserializer extends JsonDeserializer[Distribution] {

  override def deserialize(jsonElement: JsonElement, t: Type,
    context: JsonDeserializationContext): Distribution = {

    val jsonObject = jsonElement.getAsJsonObject

    val values = jsonObject.get("values").getAsJsonObject
      .entrySet.asScala
      .map { entry =>
        val distributionValue = entry.getValue.getAsJsonObject
        (entry.getKey, DistributionValue(distributionValue.get("absolute").getAsLong,
          distributionValue.get("ratio").getAsDouble))
      }
      .toMap

    Distribution(values, jsonObject.get("numberOfBins").getAsLong)
  }
}
