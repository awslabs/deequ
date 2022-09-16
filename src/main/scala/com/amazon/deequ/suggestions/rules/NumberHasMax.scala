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
import com.amazon.deequ.constraints.Constraint.complianceConstraint
import com.amazon.deequ.profiles.{ColumnProfile, NumericColumnProfile}
import com.amazon.deequ.suggestions.ConstraintSuggestion

/** If we see only numbers in a column, we suggest a corresponding maximum
  * constraint
  */
case class NumberHasMax() extends ConstraintRule[ColumnProfile] {

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
      case numericProfile: NumericColumnProfile => numericProfile.maximum.get
      case _                                    => Double.NegativeInfinity // scalastyle:ignore
    }

    val column = profile.column
    val description =
      s"""'$profile.column' values should be <= $maximum""".stripMargin
        .replaceAll("\n", "")

    val constraint = complianceConstraint(
      description,
      s"${profile.column} <= $maximum",
      Check.IsOne
    )

    val hint =
      s"""'$profile.column' should be <= $maximum""".stripMargin
        .replaceAll("\n", "")

    ConstraintSuggestion(
      constraint,
      profile.column,
      hint,
      description,
      this,
      s""".hasMax(s"$column", _ == $maximum, s"$maximum", Some(s"$hint")")""".stripMargin
        .replaceAll("\n", "")
    )
  }

  override val ruleDescription: String = "If we see numbers in a " +
    "column, we suggest a corresponding hasMax constraint"
}
