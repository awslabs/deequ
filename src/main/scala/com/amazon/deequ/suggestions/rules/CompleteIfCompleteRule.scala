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

package com.amazon.deequ.suggestions.rules

import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.Constraint.completenessConstraint
import com.amazon.deequ.profiles.ColumnProfile
import com.amazon.deequ.suggestions.ConstraintSuggestion

/** If a column is complete in the sample, we suggest a NOT NULL constraint */
case class CompleteIfCompleteRule() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    profile.completeness == 1.0
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): ConstraintSuggestion = {

    val constraint = completenessConstraint(profile.column, Check.IsOne)

    ConstraintSuggestion(
      constraint,
      profile.column,
      "Completeness: " + profile.completeness.toString,
      s"'${profile.column}' is not null",
      this,
      s""".isComplete("${profile.column}")"""
    )
  }

  override val ruleDescription: String = "If a column is complete in the sample, " +
    "we suggest a NOT NULL constraint"
}
