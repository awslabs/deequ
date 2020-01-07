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

package com.amazon.deequ.repository

import java.lang.reflect.Type

import com.amazon.deequ.analyzers.{State, _}
import org.apache.spark.sql.functions._
import com.amazon.deequ.metrics.{Distribution, Metric, _}

import util.{Failure, Success, Try}
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.google.gson._
import com.google.gson.reflect.TypeToken

import scala.collection._
import scala.collection.JavaConverters._
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import JsonSerializationConstants._

import scala.collection.JavaConversions._

private[repository] object JsonSerializationConstants {

  val STRING_MAP_TYPE: Type = new TypeToken[JList[JMap[String, Any]]]() {}.getType
  val STRING_LIST_TYPE: Type = new TypeToken[JList[String]]() {}.getType

  val ANALYZER_FIELD = "analyzer"
  val ANALYZER_NAME_FIELD = "analyzerName"
  val WHERE_FIELD = "where"
  val COLUMN_FIELD = "column"
  val COLUMNS_FIELD = "columns"
  val METRIC_MAP_FIELD = "metricMap"
  val METRIC_FIELD = "metric"
  val DATASET_DATE_FIELD = "dataSetDate"
  val TAGS_FIELD = "tags"
  val RESULT_KEY_FIELD = "resultKey"
  val ANALYZER_CONTEXT_FIELD = "analyzerContext"
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

object AnalysisResultSerde {

  def serialize(analysisResults: Seq[AnalysisResult]): String = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[ResultKey], ResultKeySerializer)
      .registerTypeAdapter(classOf[AnalysisResult], AnalysisResultSerializer)
      .registerTypeAdapter(classOf[AnalyzerContext], AnalyzerContextSerializer)
      .registerTypeAdapter(classOf[Analyzer[State[_], Metric[_]]],
        AnalyzerSerializer)
      .registerTypeAdapter(classOf[Metric[_]], MetricSerializer)
      .registerTypeAdapter(classOf[Distribution], DistributionSerializer)
      .setPrettyPrinting()
      .create

    gson.toJson(analysisResults.asJava, new TypeToken[JList[AnalysisResult]]() {}.getType)
  }

  def deserialize(analysisResults: String): Seq[AnalysisResult] = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[ResultKey], ResultKeyDeserializer)
      .registerTypeAdapter(classOf[AnalysisResult], AnalysisResultDeserializer)
      .registerTypeAdapter(classOf[AnalyzerContext], AnalyzerContextDeserializer)
      .registerTypeAdapter(classOf[Analyzer[State[_], Metric[_]]], AnalyzerDeserializer)
      .registerTypeAdapter(classOf[Metric[_]], MetricDeserializer)
      .registerTypeAdapter(classOf[Distribution], DistributionDeserializer)
      .create

    gson.fromJson(analysisResults,
      new TypeToken[JList[AnalysisResult]]() {}.getType)
        .asInstanceOf[JArrayList[AnalysisResult]].asScala
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

private[deequ] object AnalysisResultSerializer extends JsonSerializer[AnalysisResult] {

  override def serialize(analysisResult: AnalysisResult, t: Type,
    context: JsonSerializationContext): JsonElement = {

    val result = new JsonObject()

    result.add(RESULT_KEY_FIELD, context.serialize(analysisResult.resultKey, classOf[ResultKey]))

    result.add(ANALYZER_CONTEXT_FIELD,
      context.serialize(analysisResult.analyzerContext, classOf[AnalyzerContext]))

    result
  }
}

private[deequ] object AnalysisResultDeserializer extends JsonDeserializer[AnalysisResult] {

  override def deserialize(jsonElement: JsonElement, t: Type,
                           context: JsonDeserializationContext): AnalysisResult = {

    val jsonObject = jsonElement.getAsJsonObject

    val key: ResultKey = context.deserialize(jsonObject.get(RESULT_KEY_FIELD), classOf[ResultKey])
    val analyzerContext: AnalyzerContext = context
      .deserialize(jsonObject.get(ANALYZER_CONTEXT_FIELD), classOf[AnalyzerContext])

    AnalysisResult(key, analyzerContext)
  }
}

private[deequ] object AnalyzerContextSerializer extends JsonSerializer[AnalyzerContext] {

  override def serialize(analyzerContext: AnalyzerContext, t: Type,
    context: JsonSerializationContext): JsonElement = {

    val result = new JsonObject()

    val metricMap = new JsonArray()

    analyzerContext.metricMap.foreach { case (analyzer, metric) =>
      val entry = new JsonObject()

      entry.add(ANALYZER_FIELD, context.serialize(analyzer, classOf[Analyzer[State[_], Metric[_]]]))
      entry.add(METRIC_FIELD, context.serialize(metric, classOf[Metric[_]]))

      metricMap.add(entry)
    }

    result.add(METRIC_MAP_FIELD, metricMap)

    result
  }
}

private[deequ] object AnalyzerContextDeserializer extends JsonDeserializer[AnalyzerContext] {

  override def deserialize(jsonElement: JsonElement, t: Type,
    context: JsonDeserializationContext): AnalyzerContext = {

    val jsonObject = jsonElement.getAsJsonObject

    val metricMap = jsonObject.get(METRIC_MAP_FIELD).getAsJsonArray.asScala
      .map { entry =>

        val serializedAnalyzer = entry.getAsJsonObject.get(ANALYZER_FIELD)

        val analyzer = context.deserialize(serializedAnalyzer,
          classOf[Analyzer[State[_], Metric[_]]]).asInstanceOf[Analyzer[State[_], Metric[_]]]

        val metric = context.deserialize(entry.getAsJsonObject.get(METRIC_FIELD),
          classOf[Metric[_]]).asInstanceOf[Metric[_]]

        analyzer.asInstanceOf[Analyzer[State[_], Metric[_]]] -> metric
      }
      .asInstanceOf[Seq[(Analyzer[_, Metric[_]], Metric[_])]]
      .toMap

    AnalyzerContext(metricMap)
  }
}

private[deequ] object AnalyzerSerializer
  extends JsonSerializer[Analyzer[_ <: State[_], _ <: Metric[_]]] {

  override def serialize(analyzer: Analyzer[_ <: State[_], _ <: Metric[_]], t: Type,
    context: JsonSerializationContext): JsonElement = {

    val result = new JsonObject()

    analyzer match {
      case size: Size =>
        result.addProperty(ANALYZER_NAME_FIELD, "Size")
        result.addProperty(WHERE_FIELD, size.where.orNull)


      case completeness: Completeness =>
        result.addProperty(ANALYZER_NAME_FIELD, "Completeness")
        result.addProperty(COLUMN_FIELD, completeness.column)
        result.addProperty(WHERE_FIELD, completeness.where.orNull)


      case compliance: Compliance =>
        result.addProperty(ANALYZER_NAME_FIELD, "Compliance")
        result.addProperty(WHERE_FIELD, compliance.where.orNull)
        result.addProperty("instance", compliance.instance)
        result.addProperty("predicate", compliance.predicate)

      case patternMatch: PatternMatch =>
        result.addProperty(ANALYZER_NAME_FIELD, "PatternMatch")
        result.addProperty(COLUMN_FIELD, patternMatch.column)
        result.addProperty(WHERE_FIELD, patternMatch.where.orNull)
        result.addProperty("pattern", patternMatch.pattern.toString())

      case sum: Sum =>
        result.addProperty(ANALYZER_NAME_FIELD, "Sum")
        result.addProperty(COLUMN_FIELD, sum.column)
        result.addProperty(WHERE_FIELD, sum.where.orNull)

      case mean: Mean =>
        result.addProperty(ANALYZER_NAME_FIELD, "Mean")
        result.addProperty(COLUMN_FIELD, mean.column)
        result.addProperty(WHERE_FIELD, mean.where.orNull)

      case minimum: Minimum =>
        result.addProperty(ANALYZER_NAME_FIELD, "Minimum")
        result.addProperty(COLUMN_FIELD, minimum.column)
        result.addProperty(WHERE_FIELD, minimum.where.orNull)

      case maximum: Maximum =>
        result.addProperty(ANALYZER_NAME_FIELD, "Maximum")
        result.addProperty(COLUMN_FIELD, maximum.column)
        result.addProperty(WHERE_FIELD, maximum.where.orNull)

      case countDistinct: CountDistinct =>
        result.addProperty(ANALYZER_NAME_FIELD, "CountDistinct")
        result.add(COLUMNS_FIELD, context.serialize(countDistinct.columns.asJava,
          new TypeToken[JList[String]]() {}.getType))

      case distinctness: Distinctness =>
        result.addProperty(ANALYZER_NAME_FIELD, "Distinctness")
        result.add(COLUMNS_FIELD, context.serialize(distinctness.columns.asJava))

      case entropy: Entropy =>
        result.addProperty(ANALYZER_NAME_FIELD, "Entropy")
        result.addProperty(COLUMN_FIELD, entropy.column)

      case mutualInformation: MutualInformation =>
        result.addProperty(ANALYZER_NAME_FIELD, "MutualInformation")
        result.add(COLUMNS_FIELD, context.serialize(mutualInformation.columns.asJava,
          new TypeToken[JList[String]]() {}.getType))

      case uniqueValueRatio: UniqueValueRatio =>
        result.addProperty(ANALYZER_NAME_FIELD, "UniqueValueRatio")
        result.add(COLUMNS_FIELD, context.serialize(uniqueValueRatio.columns.asJava,
          new TypeToken[JList[String]]() {}.getType))

      case uniqueness: Uniqueness =>
        result.addProperty(ANALYZER_NAME_FIELD, "Uniqueness")
        result.add(COLUMNS_FIELD, context.serialize(uniqueness.columns.asJava,
          new TypeToken[JList[String]]() {}.getType))

      case histogram: Histogram if histogram.binningUdf.isEmpty =>
        result.addProperty(ANALYZER_NAME_FIELD, "Histogram")
        result.addProperty(COLUMN_FIELD, histogram.column)
        result.addProperty("maxDetailBins", histogram.maxDetailBins)

      case _ : Histogram =>
        throw new IllegalArgumentException("Unable to serialize Histogram with binningUdf!")

      case dataType: DataType =>
        result.addProperty(ANALYZER_NAME_FIELD, "DataType")
        result.addProperty(COLUMN_FIELD, dataType.column)
        result.addProperty(WHERE_FIELD, dataType.where.orNull)

      case approxCountDistinct: ApproxCountDistinct =>
        result.addProperty(ANALYZER_NAME_FIELD, "ApproxCountDistinct")
        result.addProperty(COLUMN_FIELD, approxCountDistinct.column)
        result.addProperty(WHERE_FIELD, approxCountDistinct.where.orNull)

      case correlation: Correlation =>
        result.addProperty(ANALYZER_NAME_FIELD, "Correlation")
        result.addProperty("firstColumn", correlation.firstColumn)
        result.addProperty("secondColumn", correlation.secondColumn)
        result.addProperty(WHERE_FIELD, correlation.where.orNull)

      case standardDeviation: StandardDeviation =>
        result.addProperty(ANALYZER_NAME_FIELD, "StandardDeviation")
        result.addProperty(COLUMN_FIELD, standardDeviation.column)
        result.addProperty(WHERE_FIELD, standardDeviation.where.orNull)

      case approxQuantile: ApproxQuantile =>
        result.addProperty(ANALYZER_NAME_FIELD, "ApproxQuantile")
        result.addProperty(COLUMN_FIELD, approxQuantile.column)
        result.addProperty("quantile", approxQuantile.quantile)
        result.addProperty("relativeError", approxQuantile.relativeError)

      case approxQuantiles: ApproxQuantiles =>
        result.addProperty(ANALYZER_NAME_FIELD, "ApproxQuantiles")
        result.addProperty(COLUMN_FIELD, approxQuantiles.column)
        result.addProperty("quantiles", approxQuantiles.quantiles.mkString(","))
        result.addProperty("relativeError", approxQuantiles.relativeError)


      case minLength: MinLength =>
        result.addProperty(ANALYZER_NAME_FIELD, "MinLength")
        result.addProperty(COLUMN_FIELD, minLength.column)
        result.addProperty(WHERE_FIELD, minLength.where.orNull)

      case maxLength: MaxLength =>
        result.addProperty(ANALYZER_NAME_FIELD, "MaxLength")
        result.addProperty(COLUMN_FIELD, maxLength.column)
        result.addProperty(WHERE_FIELD, maxLength.where.orNull)

      case _ =>
        throw new IllegalArgumentException(s"Unable to serialize analyzer $analyzer.")
    }

    result
  }
}

private[deequ] object AnalyzerDeserializer
  extends JsonDeserializer[Analyzer[State[_], Metric[_]]] {

  private[this] def getColumnsAsSeq(context: JsonDeserializationContext,
    json: JsonObject): Seq[String] = {

    context.deserialize(json.get(COLUMNS_FIELD), new TypeToken[JList[String]]() {}.getType)
      .asInstanceOf[JArrayList[String]].asScala
  }

  override def deserialize(jsonElement: JsonElement, t: Type,
    context: JsonDeserializationContext): Analyzer[State[_], Metric[_]] = {

    val json = jsonElement.getAsJsonObject

    val analyzer = json.get(ANALYZER_NAME_FIELD).getAsString match {

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
          None,
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
        val relativeError = json.get("relativeError").getAsDouble
        ApproxQuantile(column, quantile, relativeError)

      case "ApproxQuantiles" =>
        val column = json.get(COLUMN_FIELD).getAsString
        val quantile = json.get("quantiles").getAsString.split(",").map { _.toDouble }
        val relativeError = json.get("relativeError").getAsDouble
        ApproxQuantiles(column, quantile, relativeError)

      case "MinLength" =>
        MinLength(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json))

      case "MaxLength" =>
        MaxLength(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json))

      case analyzerName =>
        throw new IllegalArgumentException(s"Unable to deserialize analyzer $analyzerName.")
    }

    analyzer.asInstanceOf[Analyzer[State[_], Metric[_]]]
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
        throw new IllegalArgumentException(s"Unable to deserialize analyzer $metricName.")
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
