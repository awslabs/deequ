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

package com.amazon.deequ.suggestions

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.constraints.Constraint
import com.amazon.deequ.profiles.ColumnProfile
import com.amazon.deequ.suggestions.rules.ConstraintRule
import com.google.gson.{GsonBuilder, JsonArray, JsonObject}

case class ConstraintSuggestion(
    constraint: Constraint,
    columnName: String,
    currentValue: String,
    description: String,
    suggestingRule: ConstraintRule[ColumnProfile],
    codeForConstraint: String
)

object ConstraintSuggestions {

  private[this] val CONSTRANT_SUGGESTIONS_FIELD = "constraint_suggestions"

  private[suggestions] def toJson(constraintSuggestions: Seq[ConstraintSuggestion]): String = {

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

  private[suggestions] def evaluationResultsToJson(
      constraintSuggestions: Seq[ConstraintSuggestion],
      result: VerificationResult)
    : String = {

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
}
