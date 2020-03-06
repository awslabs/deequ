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

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.suggestions.rules.UniqueIfApproximatelyUniqueRule
import com.amazon.deequ.utils.FixtureSupport
import com.google.gson.JsonParser
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}

class ConstraintSuggestionResultTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "ConstraintSuggestionResult" should {

      // TODO Disabled for now as we get different results for Spark 2.2 and 2.3
//    "return Json that is formatted as expected for getColumnProfilesAsJson" in
//      withSparkSession { session =>
//
//        evaluate(session) { results =>
//
//          val columnProfilesJson = ConstraintSuggestionResult.getColumnProfilesAsJson(results)
//
//          val expectedJson =
//            """{
//              |  "columns": [
//              |    {
//              |      "column": "item",
//              |      "dataType": "Integral",
//              |      "isDataTypeInferred": "true",
//              |      "completeness": 1.0,
//              |      "approximateNumDistinctValues": 4,
//              |      "mean": 2.5,
//              |      "maximum": 4.0,
//              |      "minimum": 1.0,
//              |      "sum": 10.0,
//              |      "stdDev": 1.118033988749895,
//              |      "approxPercentiles": [
//              |        1.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        2.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        3.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0,
//              |        4.0
//              |      ]
//              |    },
//              |    {
//              |      "column": "att1",
//              |      "dataType": "String",
//              |      "isDataTypeInferred": "true",
//              |      "completeness": 1.0,
//              |      "approximateNumDistinctValues": 2,
//              |      "histogram": [
//              |        {
//              |          "value": "a",
//              |          "count": 3,
//              |          "ratio": 0.75
//              |        },
//              |        {
//              |          "value": "b",
//              |          "count": 1,
//              |          "ratio": 0.25
//              |        }
//              |      ]
//              |    },
//              |    {
//              |      "column": "att2",
//              |      "dataType": "String",
//              |      "isDataTypeInferred": "true",
//              |      "completeness": 1.0,
//              |      "approximateNumDistinctValues": 2,
//              |      "histogram": [
//              |        {
//              |          "value": "d",
//              |          "count": 1,
//              |          "ratio": 0.25
//              |        },
//              |        {
//              |          "value": "c",
//              |          "count": 3,
//              |          "ratio": 0.75
//              |        }
//              |      ]
//              |    }
//              |  ]
//              |}"""
//              .stripMargin.replaceAll("\n", "")
//
//          assertJsonStringsAreEqual(columnProfilesJson, expectedJson)
//        }
//      }

    "return Json that is formatted as expected for getConstraintSuggestionsAsJson" in
      withSparkSession { session =>

        evaluate(session) { results =>

          val constraintSuggestionJson = ConstraintSuggestionResult
            .getConstraintSuggestionsAsJson(results)

          val expectedJson =
            """{
              |  "constraint_suggestions": [
              |    {
              |      "constraint_name": "CompletenessConstraint(Completeness(att2,None))",
              |      "column_name": "att2",
              |      "current_value": "Completeness: 1.0",
              |      "description": "'att2' is not null",
              |      "suggesting_rule": "CompleteIfCompleteRule()",
              |      "rule_description": "If a column is complete in the sample, we suggest a NOT
              | NULL constraint",
              |      "code_for_constraint": ".isComplete(\"att2\")"
              |    },
              |    {
              |      "constraint_name": "CompletenessConstraint(Completeness(att1,None))",
              |      "column_name": "att1",
              |      "current_value": "Completeness: 1.0",
              |      "description": "'att1' is not null",
              |      "suggesting_rule": "CompleteIfCompleteRule()",
              |      "rule_description": "If a column is complete in the sample, we suggest a NOT
              | NULL constraint",
              |      "code_for_constraint": ".isComplete(\"att1\")"
              |    },
              |    {
              |      "constraint_name": "CompletenessConstraint(Completeness(item,None))",
              |      "column_name": "item",
              |      "current_value": "Completeness: 1.0",
              |      "description": "'item' is not null",
              |      "suggesting_rule": "CompleteIfCompleteRule()",
              |      "rule_description": "If a column is complete in the sample, we suggest a NOT
              | NULL constraint",
              |      "code_for_constraint": ".isComplete(\"item\")"
              |    },
              |    {
              |      "constraint_name": "AnalysisBasedConstraint(DataType(item,None),
              |\u003cfunction1\u003e,Some(\u003cfunction1\u003e),None)",
              |      "column_name": "item",
              |      "current_value": "DataType: Integral",
              |      "description": "'item' has type Integral",
              |      "suggesting_rule": "RetainTypeRule()",
              |      "rule_description": "If we detect a non-string type, we suggest a type
              | constraint",
              |      "code_for_constraint": ".hasDataType(\"item\", ConstrainableDataTypes
              |.Integral)"
              |    },
              |    {
              |      "constraint_name": "ComplianceConstraint(Compliance(\u0027item\u0027 has no
              | negative values,item \u003e\u003d 0,None))",
              |      "column_name": "item",
              |      "current_value": "Minimum: 1.0",
              |      "description": "\u0027item\u0027 has no negative values",
              |      "suggesting_rule": "NonNegativeNumbersRule()",
              |      "rule_description": "If we see only non-negative numbers in a column, we
              | suggest a corresponding constraint",
              |      "code_for_constraint": ".isNonNegative(\"item\")"
              |    },
              |    {
              |      "constraint_name": "UniquenessConstraint(Uniqueness(List(item),None))",
              |      "column_name": "item",
              |      "current_value": "ApproxDistinctness: 1.0",
              |      "description": "'item' is unique",
              |      "suggesting_rule": "UniqueIfApproximatelyUniqueRule()",
              |      "rule_description": "If the ratio of approximate num distinct values in a
              | column is close to the number of records (within the error of the HLL sketch),
              | we suggest a UNIQUE constraint",
              |      "code_for_constraint": ".isUnique(\"item\")"
              |    }
              |  ]
              |}"""
              .stripMargin.replaceAll("\n", "")

          assertJsonStringsAreEqual(constraintSuggestionJson, expectedJson)
        }
      }

    "return Json that is formatted as expected for getEvaluationResultsAsJson" in
      withSparkSession { session =>

        evaluateWithTrainTestSplit(session) { results =>

          val evaluationResultsJson = ConstraintSuggestionResult.getEvaluationResultsAsJson(results)

          val expectedJson =
            """{
              |  "constraint_suggestions": [
              |    {
              |      "constraint_name": "CompletenessConstraint(Completeness(att2,None))",
              |      "column_name": "att2",
              |      "current_value": "Completeness: 1.0",
              |      "description": "\u0027att2\u0027 is not null",
              |      "suggesting_rule": "CompleteIfCompleteRule()",
              |      "rule_description": "If a column is complete in the sample, we suggest a NOT
              | NULL constraint",
              |      "code_for_constraint": ".isComplete(\"att2\")",
              |      "constraint_result_on_test_set": "Failure"
              |    },
              |    {
              |      "constraint_name": "CompletenessConstraint(Completeness(att1,None))",
              |      "column_name": "att1",
              |      "current_value": "Completeness: 1.0",
              |      "description": "\u0027att1\u0027 is not null",
              |      "suggesting_rule": "CompleteIfCompleteRule()",
              |      "rule_description": "If a column is complete in the sample, we suggest a NOT
              | NULL constraint",
              |      "code_for_constraint": ".isComplete(\"att1\")",
              |      "constraint_result_on_test_set": "Failure"
              |    },
              |    {
              |      "constraint_name": "CompletenessConstraint(Completeness(item,None))",
              |      "column_name": "item",
              |      "current_value": "Completeness: 1.0",
              |      "description": "\u0027item\u0027 is not null",
              |      "suggesting_rule": "CompleteIfCompleteRule()",
              |      "rule_description": "If a column is complete in the sample, we suggest a NOT
              | NULL constraint",
              |      "code_for_constraint": ".isComplete(\"item\")",
              |      "constraint_result_on_test_set": "Failure"
              |    },
              |    {
              |      "constraint_name": "AnalysisBasedConstraint(DataType(item,None),
              |\u003cfunction1\u003e,Some(\u003cfunction1\u003e),None)",
              |      "column_name": "item",
              |      "current_value": "DataType: Integral",
              |      "description": "\u0027item\u0027 has type Integral",
              |      "suggesting_rule": "RetainTypeRule()",
              |      "rule_description": "If we detect a non-string type, we suggest a type
              | constraint",
              |      "code_for_constraint": ".hasDataType(\"item\", ConstrainableDataTypes
              |.Integral)",
              |      "constraint_result_on_test_set": "Failure"
              |    },
              |    {
              |      "constraint_name": "ComplianceConstraint(Compliance(\u0027item\u0027 has no
              | negative values,item \u003e\u003d 0,None))",
              |      "column_name": "item",
              |      "current_value": "Minimum: 1.0",
              |      "description": "\u0027item\u0027 has no negative values",
              |      "suggesting_rule": "NonNegativeNumbersRule()",
              |      "rule_description": "If we see only non-negative numbers in a column, we
              | suggest a corresponding constraint",
              |      "code_for_constraint": ".isNonNegative(\"item\")",
              |      "constraint_result_on_test_set": "Failure"
              |    },
              |    {
              |      "constraint_name": "UniquenessConstraint(Uniqueness(List(item),None))",
              |      "column_name": "item",
              |      "current_value": "ApproxDistinctness: 1.0",
              |      "description": "\u0027item\u0027 is unique",
              |      "suggesting_rule": "UniqueIfApproximatelyUniqueRule()",
              |      "rule_description": "If the ratio of approximate num distinct values in a
              | column is close to the number of records (within the error of the HLL sketch),
              | we suggest a UNIQUE constraint",
              |      "code_for_constraint": ".isUnique(\"item\")",
              |      "constraint_result_on_test_set": "Failure"
              |    }
              |  ]
              |}"""
              .stripMargin.replaceAll("\n", "")

          assertJsonStringsAreEqual(evaluationResultsJson, expectedJson)
        }
      }

    "return Json that is formatted as expected for getEvaluationResultsAsJson without" +
      " train/test-split" in withSparkSession { session =>
        evaluate(session) { results =>

          val evaluationResultsJson = ConstraintSuggestionResult.getEvaluationResultsAsJson(results)

          val expectedJson =
            """{
              |  "constraint_suggestions": [
              |    {
              |      "constraint_name": "CompletenessConstraint(Completeness(att2,None))",
              |      "column_name": "att2",
              |      "current_value": "Completeness: 1.0",
              |      "description": "\u0027att2\u0027 is not null",
              |      "suggesting_rule": "CompleteIfCompleteRule()",
              |      "rule_description": "If a column is complete in the sample, we suggest a NOT
              | NULL constraint",
              |      "code_for_constraint": ".isComplete(\"att2\")",
              |      "constraint_result_on_test_set": "Unknown"
              |    },
              |    {
              |      "constraint_name": "CompletenessConstraint(Completeness(att1,None))",
              |      "column_name": "att1",
              |      "current_value": "Completeness: 1.0",
              |      "description": "\u0027att1\u0027 is not null",
              |      "suggesting_rule": "CompleteIfCompleteRule()",
              |      "rule_description": "If a column is complete in the sample, we suggest a NOT
              | NULL constraint",
              |      "code_for_constraint": ".isComplete(\"att1\")",
              |      "constraint_result_on_test_set": "Unknown"
              |    },
              |    {
              |      "constraint_name": "CompletenessConstraint(Completeness(item,None))",
              |      "column_name": "item",
              |      "current_value": "Completeness: 1.0",
              |      "description": "\u0027item\u0027 is not null",
              |      "suggesting_rule": "CompleteIfCompleteRule()",
              |      "rule_description": "If a column is complete in the sample, we suggest a NOT
              | NULL constraint",
              |      "code_for_constraint": ".isComplete(\"item\")",
              |      "constraint_result_on_test_set": "Unknown"
              |    },
              |    {
              |      "constraint_name": "AnalysisBasedConstraint(DataType(item,None),
              |\u003cfunction1\u003e,Some(\u003cfunction1\u003e),None)",
              |      "column_name": "item",
              |      "current_value": "DataType: Integral",
              |      "description": "\u0027item\u0027 has type Integral",
              |      "suggesting_rule": "RetainTypeRule()",
              |      "rule_description": "If we detect a non-string type, we suggest a type
              | constraint",
              |      "code_for_constraint": ".hasDataType(\"item\", ConstrainableDataTypes
              |.Integral)",
              |      "constraint_result_on_test_set": "Unknown"
              |    },
              |    {
              |      "constraint_name": "ComplianceConstraint(Compliance(\u0027item\u0027 has no
              | negative values,item \u003e\u003d 0,None))",
              |      "column_name": "item",
              |      "current_value": "Minimum: 1.0",
              |      "description": "\u0027item\u0027 has no negative values",
              |      "suggesting_rule": "NonNegativeNumbersRule()",
              |      "rule_description": "If we see only non-negative numbers in a column, we
              | suggest a corresponding constraint",
              |      "code_for_constraint": ".isNonNegative(\"item\")",
              |      "constraint_result_on_test_set": "Unknown"
              |    },
              |    {
              |      "constraint_name": "UniquenessConstraint(Uniqueness(List(item),None))",
              |      "column_name": "item",
              |      "current_value": "ApproxDistinctness: 1.0",
              |      "description": "\u0027item\u0027 is unique",
              |      "suggesting_rule": "UniqueIfApproximatelyUniqueRule()",
              |      "rule_description": "If the ratio of approximate num distinct values in a
              | column is close to the number of records (within the error of the HLL sketch),
              | we suggest a UNIQUE constraint",
              |      "code_for_constraint": ".isUnique(\"item\")",
              |      "constraint_result_on_test_set": "Unknown"
              |    }
              |  ]
              |}"""
              .stripMargin.replaceAll("\n", "")

          assertJsonStringsAreEqual(evaluationResultsJson, expectedJson)
        }
      }
  }

  private[this] def evaluate(session: SparkSession)(test: ConstraintSuggestionResult => Unit)
    : Unit = {

    val data = getDfFull(session)

    val results = ConstraintSuggestionRunner()
      .onData(data)
      .addConstraintRules(Rules.DEFAULT)
      .addConstraintRule(UniqueIfApproximatelyUniqueRule())
      .run()

    test(results)
  }

  private[this] def evaluateWithTrainTestSplit(session: SparkSession)(
      test: ConstraintSuggestionResult => Unit)
    : Unit = {

    val data = getDfFull(session)

    val results = ConstraintSuggestionRunner()
      .onData(data)
      .addConstraintRules(Rules.DEFAULT)
      .addConstraintRule(UniqueIfApproximatelyUniqueRule())
      .useTrainTestSplitWithTestsetRatio(0.1, Some(0))
      .run()

    test(results)
  }

  private[this] def assertJsonStringsAreEqual(jsonA: String, jsonB: String): Unit = {

    val parser = new JsonParser()

    assert(parser.parse(jsonA) == parser.parse(jsonB))
  }

}
