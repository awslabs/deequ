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

import com.amazon.deequ.checks.{Check, CheckStatus}
import com.amazon.deequ.profiles.{ColumnProfile, NumericColumnProfile}
import com.amazon.deequ.{ComputedStatistics, VerificationResult}
import com.amazon.deequ.repository.{ResultKey, StatisticsResult}
import com.amazon.deequ.statistics.Statistic
import com.amazon.deequ.suggestions.{ConstraintSuggestion, ConstraintSuggestionResult}
import com.google.gson.{GsonBuilder, JsonArray, JsonObject, JsonPrimitive}

private[deequ] object JsonSerializer {

  private val DATASET_DATE_FIELD = "dataset_date"

  def verificationResult(
      verificationResult: VerificationResult,
      forAnalyzers: Seq[Statistic] = Seq.empty)
    : String = {

    val metricsAsAnalyzerContext = ComputedStatistics(verificationResult.metrics)

    JsonSerializer.computedStatistics(metricsAsAnalyzerContext, forAnalyzers)
  }

  def statisticsResult(
      analysisResult: StatisticsResult,
      forAnalyzers: Seq[Statistic] = Seq.empty,
      withTags: Seq[String] = Seq.empty)
   : String = {

    var serializableResult = SimpleResultSerde.deserialize(
      computedStatistics(analysisResult.computedStatistics, forAnalyzers))
      .asInstanceOf[Seq[Map[String, Any]]]

    serializableResult = addColumnToSerializableResult(
      serializableResult, DATASET_DATE_FIELD, analysisResult.resultKey.dataSetDate)

    analysisResult.resultKey.tags
      .filterKeys(tagName => withTags.isEmpty || withTags.contains(tagName))
      .map { case (tagName, tagValue) =>
        (formatTagColumnNameInJson(tagName, serializableResult), tagValue)}
      .foreach { case (key, value) => serializableResult = addColumnToSerializableResult(
        serializableResult, key, value)
      }

    SimpleResultSerde.serialize(serializableResult)
  }

  private[this] def formatTagColumnNameInJson(
      tagName: String,
      serializableResult: Seq[Map[String, Any]])
    : String = {

    var tagColumnName = tagName.replaceAll("[^A-Za-z0-9_]", "").toLowerCase

    if (serializableResult.headOption.nonEmpty) {
      if (serializableResult.head.keySet.contains(tagColumnName)) {
        tagColumnName = tagColumnName + "_2"
      }
    }
    tagColumnName
  }

  private[this] def addColumnToSerializableResult(
     serializableResult: Seq[Map[String, Any]],
     tagName: String,
     serializableTagValue: Any)
   : Seq[Map[String, Any]] = {

    if (serializableResult.headOption.nonEmpty &&
      !serializableResult.head.keySet.contains(tagName)) {

      serializableResult.map {
        map => map + (tagName -> serializableTagValue)
      }
    } else {
      serializableResult
    }
  }

  def metricsRepositoryResults(results: Seq[StatisticsResult], withTags: Seq[String] = Seq.empty): String = {

    if (results.isEmpty) {
      // Handle this case exactly like directly calling the method on AnalysisResult
      statisticsResult(StatisticsResult(ResultKey(0, Map.empty), ComputedStatistics.empty))
    } else {
      results
        .map { result => statisticsResult(result, withTags = withTags) }
        .reduce(jsonUnion)
    }
  }

  def computedStatistics(
      analyzerContext: ComputedStatistics,
      forAnalyzers: Seq[Statistic] = Seq.empty)
    : String = {

    val metricsList = getSimplifiedMetricOutputForSelectedAnalyzers(analyzerContext, forAnalyzers)

    val result = metricsList.map { simplifiedMetricOutput =>
      Map(
        "entity" -> simplifiedMetricOutput.entity,
        "instance" -> simplifiedMetricOutput.instance,
        "name" -> simplifiedMetricOutput.name,
        "value" -> simplifiedMetricOutput.value
      )
    }

    SimpleResultSerde.serialize(result)
  }

  private[this] def getSimplifiedMetricOutputForSelectedAnalyzers(
      analyzerContext: ComputedStatistics,
      forAnalyzers: Seq[Statistic])
    : Seq[SimpleMetricOutput] = {

    val selectedMetrics = analyzerContext.metricMap
      .filterKeys(analyzer => forAnalyzers.isEmpty || forAnalyzers.contains(analyzer))
      .values
      .toSeq

    val metricsList = selectedMetrics
      .filter(_.value.isSuccess) // Get statistics with successful results
      .flatMap(_.flatten()) // Get metrics as double
      .map { doubleMetric =>
      SimpleMetricOutput(
        doubleMetric.entity.toString,
        doubleMetric.instance,
        doubleMetric.name,
        doubleMetric.value.get
      )
    }

    metricsList
  }

  private[this] case class SimpleMetricOutput(
      entity: String,
      instance: String,
      name: String,
      value: Double)


  def jsonUnion(jsonOne: String, jsonTwo: String): String = {

    val objectOne: Seq[Map[String, Any]] = SimpleResultSerde.deserialize(jsonOne)
    val objectTwo: Seq[Map[String, Any]] = SimpleResultSerde.deserialize(jsonTwo)

    val columnsTotal = objectOne.headOption.getOrElse(Map.empty).keySet ++
      objectTwo.headOption.getOrElse(Map.empty).keySet

    val unioned = (objectTwo ++ objectOne).map { map =>

      var columnsToAdd = Map.empty[String, Any]

      columnsTotal.diff(map.keySet).foreach { missingColumn =>
        columnsToAdd = columnsToAdd + (missingColumn -> null)
      }

      map ++ columnsToAdd
    }

    SimpleResultSerde.serialize(unioned)
  }


  def checkResultsAsJson(
      verificationResult: VerificationResult,
      forChecks: Seq[Check] = Seq.empty)
    : String = {

    val simplifiedCheckResults = getSimplifiedCheckResultOutput(verificationResult)

    val checkResults = simplifiedCheckResults
      .map { simpleCheckResultOutput =>
        Map(
          "check" -> simpleCheckResultOutput.checkDescription,
          "check_level" -> simpleCheckResultOutput.checkLevel,
          "check_status" -> simpleCheckResultOutput.checkStatus,
          "constraint" -> simpleCheckResultOutput.constraint,
          "constraint_status" -> simpleCheckResultOutput.constraintStatus,
          "constraint_message" -> simpleCheckResultOutput.constraintMessage
        )
      }

    SimpleResultSerde.serialize(checkResults)
  }

  private[this] def getSimplifiedCheckResultOutput(
      verificationResult: VerificationResult)
    : Seq[SimpleCheckResultOutput] = {

    val selectedCheckResults = verificationResult.checkResults
      .values
      .toSeq

    selectedCheckResults
      .flatMap { checkResult =>
        checkResult.constraintResults.map { constraintResult =>
          SimpleCheckResultOutput(
            checkResult.check.description,
            checkResult.check.level.toString,
            checkResult.status.toString,

            constraintResult.constraint.toString,
            constraintResult.status.toString,
            constraintResult.message.getOrElse("")
          )
        }
      }
  }

  private[this] case class SimpleCheckResultOutput(
      checkDescription: String,
      checkLevel: String,
      checkStatus: String,
      constraint: String,
      constraintStatus: String,
      constraintMessage: String)


  private[this] val CONSTRANT_SUGGESTIONS_FIELD = "constraint_suggestions"

  private[deequ] def constraintSuggestions(constraintSuggestionResult: ConstraintSuggestionResult): String = {

    val constraintSuggestions = constraintSuggestionResult.constraintSuggestions.values.flatten.toSeq
    val json = new JsonObject()

    val constraintsJson = new JsonArray()

    constraintSuggestions.foreach { constraintSuggestion =>

      val constraintJson = new JsonObject()
      addSharedProperties(constraintJson, constraintSuggestion)

      constraintsJson.add(constraintJson)
    }

    json.add(CONSTRANT_SUGGESTIONS_FIELD, constraintsJson)

    val gson = new GsonBuilder()
      .setPrettyPrinting()
      .create()

    gson.toJson(json)
  }

  private[deequ] def evaluationResults(constraintSuggestionResult: ConstraintSuggestionResult): String = {

    val constraintSuggestions = constraintSuggestionResult.constraintSuggestions.values.flatten.toSeq
    val result = constraintSuggestionResult.verificationResult.getOrElse(
      VerificationResult(CheckStatus.Warning, Map.empty, Map.empty))

    val constraintResults = result.checkResults
      .map { case (_, checkResult) => checkResult }
      .headOption.map { checkResult =>
      checkResult.constraintResults
    }
      .getOrElse(Seq.empty)

    val json = new JsonObject()

    val constraintEvaluations = new JsonArray()

    val constraintResultsOnTestSet = constraintResults.map { checkResult =>
      checkResult.status.toString
    }

    constraintSuggestions.zipAll(constraintResultsOnTestSet, null, "Unknown")
      .foreach { case (constraintSuggestion, constraintResult) =>

        val constraintEvaluation = new JsonObject()
        addSharedProperties(constraintEvaluation, constraintSuggestion)

        constraintEvaluation.addProperty("constraint_result_on_test_set",
          constraintResult)

        constraintEvaluations.add(constraintEvaluation)
      }

    json.add(CONSTRANT_SUGGESTIONS_FIELD, constraintEvaluations)

    val gson = new GsonBuilder()
      .setPrettyPrinting()
      .create()

    gson.toJson(json)
  }

  private[this] def addSharedProperties(
      jsonObject: JsonObject,
      constraintSuggestion: ConstraintSuggestion)
    : Unit = {

    jsonObject.addProperty("constraint_name", constraintSuggestion.constraint.toString)
    jsonObject.addProperty("column_name", constraintSuggestion.columnName)
    jsonObject.addProperty("current_value", constraintSuggestion.currentValue)
    jsonObject.addProperty("description", constraintSuggestion.description)
    jsonObject.addProperty("suggesting_rule", constraintSuggestion.suggestingRule.toString)
    jsonObject.addProperty("rule_description", constraintSuggestion.suggestingRule.ruleDescription)
    jsonObject.addProperty("code_for_constraint", constraintSuggestion.codeForConstraint)
  }

  def columnsProfiles(columnProfiles: Seq[ColumnProfile]): String = {

    val json = new JsonObject()

    val columns = new JsonArray()

    columnProfiles.foreach { profile =>

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
