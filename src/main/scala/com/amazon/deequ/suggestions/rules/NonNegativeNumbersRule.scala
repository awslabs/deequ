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
import com.amazon.deequ.constraints.Constraint.complianceConstraint
import com.amazon.deequ.profiles.{ColumnProfile, NumericColumnProfile}
import com.amazon.deequ.suggestions.ConstraintSuggestion

/** If we see only non-negative numbers in a column, we suggest a corresponding constraint */
case class NonNegativeNumbersRule() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    profile match {
      case numericProfile: NumericColumnProfile => numericProfile.minimum.exists(_ >= 0.0)
      case _ => false
    }
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): ConstraintSuggestion = {

    val description = s"'${profile.column}' has no negative values"
    val constraint = complianceConstraint(description, s"${profile.column} >= 0", Check.IsOne)

    val minimum = profile match {
      case numericProfile: NumericColumnProfile
        if numericProfile.minimum.isDefined => numericProfile.minimum.get.toString
      case _ => "Error while calculating minimum!"
    }

    ConstraintSuggestion(
      constraint,
      profile.column,
      "Minimum: " + minimum,
      description,
      this,
      s""".isNonNegative("${profile.column}")"""
    )
  }

  override val ruleDescription: String = "If we see only non-negative numbers in a " +
    "column, we suggest a corresponding constraint"
}
