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
case class NumberIsContainedInRule() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(
      profile: ColumnProfile,
      numRecords: Long
  ): Boolean = {
    profile match {
      case numericProfile: NumericColumnProfile =>
        numericProfile.minimum.isDefined && numericProfile.maximum.isDefined
      case _ => false
    }
  }

  override def candidate(
      profile: ColumnProfile,
      numRecords: Long
  ): ConstraintSuggestion = {

    val column = profile.column
    val description =
      s"""'${column}' values should be contained within the specified inclusive interval
        s"[minimum, maximum]""".stripMargin.replaceAll("\n", "")

    val (minimum, maximum) = profile match {
      case numericProfile: NumericColumnProfile
          if numericProfile.minimum.isDefined && numericProfile.maximum.isDefined =>
        (
          numericProfile.minimum.get.toString,
          numericProfile.maximum.get.toString
        )
    }

    val constraint = complianceConstraint(
      description,
      s"${profile.column} >= minimum",
      Check.IsOne
    )

    val hint =
      s"""${minimum} <= ${column} <= ${maximum} should be contained within the inclusive interval
        [${maximum}, ${minimum}]""".stripMargin.replaceAll("\n", "")

    ConstraintSuggestion(
      constraint,
      profile.column,
      s"""$minimum <= '$profile.column' <= $maximum""",
      description,
      this,
      s""".isContainedIn("$column", $minimum, $maximum, Some("${hint}")")""".stripMargin
        .replaceAll("\n", "")
    )
  }

  override val ruleDescription: String = "If we see numbers in a " +
    "column, we suggest a corresponding isContainedIn constraint"
}
