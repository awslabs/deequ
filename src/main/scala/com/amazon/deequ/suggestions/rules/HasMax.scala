/** Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  * http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  */

package com.amazon.deequ.suggestions.rules

import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.Constraint.maxConstraint
import com.amazon.deequ.profiles.{ColumnProfile, NumericColumnProfile}
import com.amazon.deequ.suggestions.ConstraintSuggestion
import com.amazon.deequ.checks

/** If we see only non-negative numbers in a column, we suggest a corresponding
  * constraint
  */
case class HasMax() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(
      profile: ColumnProfile,
      numRecords: Long
  ): Boolean = {
    profile match {
      case numericProfile: NumericColumnProfile =>
        numericProfile.maximum.isDefined
      case _ => false
    }
  }

  override def candidate(
      profile: ColumnProfile,
      numRecords: Long
  ): ConstraintSuggestion = {

    val maximum: Double = profile match {
      // we only generate a candidate if this is a numeric profile and max isDefined
      case numericProfile: NumericColumnProfile => numericProfile.maximum.get
    }

    val description = s"'${profile.column}' <= $maximum"
    val constraint = maxConstraint(profile.column, _ == maximum)

    ConstraintSuggestion(
      constraint,
      profile.column,
      s"Maximum: $maximum",
      description,
      this,
      s""".hasMax("${profile.column}", _ == $maximum)"""
    )
  }

  override val ruleDescription: String = "If we see a numeric column" +
    "we suggest a corresponding Maximum value constraint"
}
