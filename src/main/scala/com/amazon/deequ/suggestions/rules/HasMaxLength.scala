/**
 * Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.deequ.constraints.Constraint.maxLengthConstraint
import com.amazon.deequ.profiles.ColumnProfile
import com.amazon.deequ.profiles.StringColumnProfile
import com.amazon.deequ.suggestions.CommonConstraintSuggestion
import com.amazon.deequ.suggestions.ConstraintSuggestion

case class HasMaxLength() extends ConstraintRule[ColumnProfile] {
  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    profile match {
      case profile: StringColumnProfile => profile.maxLength.isDefined
      case _ => false
    }
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): ConstraintSuggestion = {
    val maxLength: Double = profile.asInstanceOf[StringColumnProfile].maxLength.get

    val constraint = maxLengthConstraint(profile.column, _ <= maxLength)

    CommonConstraintSuggestion(
      constraint,
      profile.column,
      "MaxLength: " + profile.completeness.toString,
      s"The length of '${profile.column}' <= $maxLength",
      this,
      s""".hasMaxLength("${profile.column}", _ <= $maxLength)"""
    )
  }

  override val ruleDescription: String = "If we see a string column, " +
    "we suggest a corresponding Maximum length constraint"
}
