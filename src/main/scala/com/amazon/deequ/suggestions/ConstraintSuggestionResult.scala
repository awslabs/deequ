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
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.profiles.{ColumnProfile, ColumnProfiles}

/**
  * The result returned from the ConstraintSuggestionSuite
  *
  * @param columnProfiles The column profiles
  * @param numRecordsUsedForProfiling The number of records that were used for computing
  *                                   the column profiles
  * @param constraintSuggestions The suggested constraints
  * @param verificationResult The verificationResult in case a train/test split was used
  */
case class ConstraintSuggestionResult(
  columnProfiles: Map[String, ColumnProfile],
  numRecordsUsedForProfiling: Long,
  constraintSuggestions: Map[String, Seq[ConstraintSuggestion]],
  verificationResult: Option[VerificationResult] = None)


object ConstraintSuggestionResult {

  def getColumnProfilesAsJson(constraintSuggestionResult: ConstraintSuggestionResult): String = {

    ColumnProfiles
      .toJson(constraintSuggestionResult.columnProfiles.values.toSeq)
  }

  def getConstraintSuggestionsAsJson(constraintSuggestionResult: ConstraintSuggestionResult)
    : String = {
    ConstraintSuggestions
      .toJson(constraintSuggestionResult.constraintSuggestions.values.fold(Seq.empty)( _ ++ _))
  }

  def getEvaluationResultsAsJson(constraintSuggestionResult: ConstraintSuggestionResult)
    : String = {

    ConstraintSuggestions
      .evaluationResultsToJson(
        constraintSuggestionResult.constraintSuggestions.values.fold(Seq.empty)( _ ++ _),
        constraintSuggestionResult.verificationResult.getOrElse(
          VerificationResult(CheckStatus.Warning, Map.empty, Map.empty)))
  }
}
