/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.deequ.constraints.Constraint.minConstraint
import com.amazon.deequ.profiles.ColumnProfile
import com.amazon.deequ.profiles.NumericColumnProfile
import com.amazon.deequ.suggestions.ConstraintSuggestion

/** If we see only non-negative numbers in a column, we suggest a corresponding
  * constraint
  */
case class HasMin() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    profile match {
      case np: NumericColumnProfile => np.minimum.isDefined
      case _ => false
    }
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): ConstraintSuggestion = {
    val minimum: Double = profile match { case np: NumericColumnProfile => np.minimum.get }

    val description = s"'${profile.column}' >= $minimum"
    val constraint = minConstraint(profile.column, _ == minimum)

    ConstraintSuggestion(
      constraint,
      profile.column,
      s"Minimum: $minimum",
      description,
      this,
      s""".hasMin("${profile.column}", _ == $minimum)"""
    )
  }

  override val ruleDescription: String = "If we see a numeric column, " +
    "we suggest a corresponding Minimum value constraint"
}
