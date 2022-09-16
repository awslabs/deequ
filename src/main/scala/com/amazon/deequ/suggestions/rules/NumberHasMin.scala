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
case class NumberHasMin() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(
      profile: ColumnProfile,
      numRecords: Long
  ): Boolean = {
    profile match {
      case numericProfile: NumericColumnProfile =>
        numericProfile.minimum.exists(
          _ > Double.NegativeInfinity
        ) && numericProfile.maximum.exists(_ < Double.PositiveInfinity)
      case _ => false
    }
  }

  override def candidate(
      profile: ColumnProfile,
      numRecords: Long
  ): ConstraintSuggestion = {

    val minimum: Double = profile match {
      case numericProfile: NumericColumnProfile
          if numericProfile.minimum.isDefined =>
        numericProfile.minimum.get
      case _ => Double.NegativeInfinity
    }

    val column = profile.column
    val description =
      s"""'$profile.column' values should be >= $minimum""".stripMargin
        .replaceAll("\n", "")

    // hasMin(
    //   column: String,
    //   assertion: Double => Boolean,
    //   hint: Option[String] = None)

    val constraint = complianceConstraint(
      description,
      s"${profile.column} >= $minimum",
      Check.IsOne
    )

    val hint =
      s"""'$profile.column' should be >= $minimum""".stripMargin
        .replaceAll("\n", "")

    ConstraintSuggestion(
      constraint,
      profile.column,
      hint,
      description,
      this,
      s""".hasMin(s"$column", _ == $minimum, Some(s"$hint")")""".stripMargin
        .replaceAll("\n", "")
    )
  }

  override val ruleDescription: String = "If we see numbers in a " +
    "column, we suggest a corresponding hasMin constraint"
}
