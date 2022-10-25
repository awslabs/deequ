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
import com.amazon.deequ.constraints.Constraint.meanConstraint
import com.amazon.deequ.profiles.{ColumnProfile, NumericColumnProfile}
import com.amazon.deequ.suggestions.ConstraintSuggestion
import com.amazon.deequ.checks

/** If we see only non-negative numbers in a column, we suggest a corresponding
  * constraint
  */
case class HasMean() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(
      profile: ColumnProfile,
      numRecords: Long
  ): Boolean = {
    profile match {
      // scalastyle:off
      case numericProfile: NumericColumnProfile => numericProfile.mean.isDefined
      case _                                    => false
      // scalastyle:on
    }
  }

  override def candidate(
      profile: ColumnProfile,
      numRecords: Long
  ): ConstraintSuggestion = {

    val mean: Double = profile match {
      // we only generate a candidate if this is a numeric profile and max isDefined
      case numericProfile: NumericColumnProfile => numericProfile.maximum.get
    }

    val description = s"'${profile.column}' <= $mean"
    val constraint = meanConstraint(profile.column, _ == mean)

    ConstraintSuggestion(
      constraint,
      profile.column,
      s"Mean: $mean",
      description,
      this,
      s""".hasMean("${profile.column}", _ == $mean)"""
    )
  }

  override val ruleDescription: String = "If we see a numeric column, " +
    "we suggest a corresponding Mean value constraint"
}
