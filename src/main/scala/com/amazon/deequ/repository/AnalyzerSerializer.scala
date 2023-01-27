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
  JsonElement,
  JsonObject,
  JsonSerializationContext,
  JsonSerializer
}
import com.google.gson.reflect.TypeToken

import java.lang.reflect.Type
import java.util.{List => JList}
import scala.collection.JavaConverters._

object AnalyzerSerializer
    extends JsonSerializer[Analyzer[_ <: State[_], _ <: Metric[_]]] {

  override def serialize(
      analyzer: Analyzer[_ <: State[_], _ <: Metric[_]],
      t: Type,
      context: JsonSerializationContext
  ): JsonElement = {

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
        result.add(
          COLUMNS_FIELD,
          context.serialize(
            countDistinct.columns.asJava,
            new TypeToken[JList[String]]() {}.getType
          )
        )

      case distinctness: Distinctness =>
        result.addProperty(ANALYZER_NAME_FIELD, "Distinctness")
        result.add(
          COLUMNS_FIELD,
          context.serialize(distinctness.columns.asJava)
        )

      case entropy: Entropy =>
        result.addProperty(ANALYZER_NAME_FIELD, "Entropy")
        result.addProperty(COLUMN_FIELD, entropy.column)

      case mutualInformation: MutualInformation =>
        result.addProperty(ANALYZER_NAME_FIELD, "MutualInformation")
        result.add(
          COLUMNS_FIELD,
          context.serialize(
            mutualInformation.columns.asJava,
            new TypeToken[JList[String]]() {}.getType
          )
        )

      case uniqueValueRatio: UniqueValueRatio =>
        result.addProperty(ANALYZER_NAME_FIELD, "UniqueValueRatio")
        result.add(
          COLUMNS_FIELD,
          context.serialize(
            uniqueValueRatio.columns.asJava,
            new TypeToken[JList[String]]() {}.getType
          )
        )

      case uniqueness: Uniqueness =>
        result.addProperty(ANALYZER_NAME_FIELD, "Uniqueness")
        result.add(
          COLUMNS_FIELD,
          context.serialize(
            uniqueness.columns.asJava,
            new TypeToken[JList[String]]() {}.getType
          )
        )

      case histogram: Histogram if histogram.binningUdf.isEmpty =>
        result.addProperty(ANALYZER_NAME_FIELD, "Histogram")
        result.addProperty(COLUMN_FIELD, histogram.column)
        result.addProperty("maxDetailBins", histogram.maxDetailBins)

      case _: Histogram =>
        throw new IllegalArgumentException(
          "Unable to serialize Histogram with binningUdf!"
        )

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
        throw new IllegalArgumentException(
          s"Unable to serialize analyzer $analyzer."
        )
    }

    result
  }
}
