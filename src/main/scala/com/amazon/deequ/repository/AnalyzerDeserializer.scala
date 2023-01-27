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

import com.amazon.deequ.analyzers.{
  Analyzer,
  ApproxCountDistinct,
  ApproxQuantile,
  ApproxQuantiles,
  Completeness,
  Compliance,
  Correlation,
  CountDistinct,
  DataType,
  Distinctness,
  Entropy,
  Histogram,
  MaxLength,
  Maximum,
  Mean,
  MinLength,
  Minimum,
  MutualInformation,
  PatternMatch,
  Size,
  StandardDeviation,
  State,
  Sum,
  UniqueValueRatio,
  Uniqueness
}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.JsonSerializationConstants.{
  ANALYZER_NAME_FIELD,
  COLUMNS_FIELD,
  COLUMN_FIELD,
  WHERE_FIELD
}
import com.google.gson.{
  JsonDeserializationContext,
  JsonDeserializer,
  JsonElement,
  JsonObject
}
import com.google.gson.reflect.TypeToken
import java.util.{ArrayList => JArrayList, List => JList}
import java.lang.reflect.Type
import scala.collection.Seq
import scala.collection.JavaConverters._

object AnalyzerDeserializer
    extends JsonDeserializer[Analyzer[State[_], Metric[_]]] {

  private[this] def getColumnsAsSeq(
      context: JsonDeserializationContext,
      json: JsonObject
  ): Seq[String] = {

    context
      .deserialize(
        json.get(COLUMNS_FIELD),
        new TypeToken[JList[String]]() {}.getType
      )
      .asInstanceOf[JArrayList[String]]
      .asScala
  }

  override def deserialize(
      jsonElement: JsonElement,
      t: Type,
      context: JsonDeserializationContext
  ): Analyzer[State[_], Metric[_]] = {

    val json = jsonElement.getAsJsonObject

    val analyzer = json.get(ANALYZER_NAME_FIELD).getAsString match {

      case "Size" =>
        Size(getOptionalWhereParam(json))

      case "Completeness" =>
        Completeness(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json)
        )

      case "Compliance" =>
        Compliance(
          json.get("instance").getAsString,
          json.get("predicate").getAsString,
          getOptionalWhereParam(json)
        )

      case "PatternMatch" =>
        PatternMatch(
          json.get(COLUMN_FIELD).getAsString,
          json.get("pattern").getAsString.r,
          getOptionalWhereParam(json)
        )

      case "Sum" =>
        Sum(json.get(COLUMN_FIELD).getAsString, getOptionalWhereParam(json))

      case "Mean" =>
        Mean(json.get(COLUMN_FIELD).getAsString, getOptionalWhereParam(json))

      case "Minimum" =>
        Minimum(json.get(COLUMN_FIELD).getAsString, getOptionalWhereParam(json))

      case "Maximum" =>
        Maximum(json.get(COLUMN_FIELD).getAsString, getOptionalWhereParam(json))

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
          json.get("maxDetailBins").getAsInt
        )

      case "DataType" =>
        DataType(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json)
        )

      case "ApproxCountDistinct" =>
        ApproxCountDistinct(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json)
        )

      case "Correlation" =>
        Correlation(
          json.get("firstColumn").getAsString,
          json.get("secondColumn").getAsString,
          getOptionalWhereParam(json)
        )

      case "StandardDeviation" =>
        StandardDeviation(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json)
        )

      case "ApproxQuantile" =>
        val column = json.get(COLUMN_FIELD).getAsString
        val quantile = json.get("quantile").getAsDouble
        val relativeError = json.get("relativeError").getAsDouble
        ApproxQuantile(column, quantile, relativeError)

      case "ApproxQuantiles" =>
        val column = json.get(COLUMN_FIELD).getAsString
        val quantile =
          json.get("quantiles").getAsString.split(",").map { _.toDouble }
        val relativeError = json.get("relativeError").getAsDouble
        ApproxQuantiles(column, quantile, relativeError)

      case "MinLength" =>
        MinLength(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json)
        )

      case "MaxLength" =>
        MaxLength(
          json.get(COLUMN_FIELD).getAsString,
          getOptionalWhereParam(json)
        )

      case analyzerName =>
        throw new IllegalArgumentException(
          s"Unable to deserialize analyzer $analyzerName."
        )
    }

    analyzer.asInstanceOf[Analyzer[State[_], Metric[_]]]
  }

  private[this] def getOptionalWhereParam(
      jsonObject: JsonObject
  ): Option[String] = {
    if (jsonObject.has(WHERE_FIELD)) {
      Option(jsonObject.get(WHERE_FIELD).getAsString)
    } else {
      None
    }
  }
}
