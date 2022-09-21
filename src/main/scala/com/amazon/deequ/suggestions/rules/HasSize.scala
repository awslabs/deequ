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
import com.amazon.deequ.constraints.Constraint.sizeConstraint
import com.amazon.deequ.constraints.Constraint.complianceConstraint
import com.amazon.deequ.profiles.{ColumnProfile, NumericColumnProfile}
import com.amazon.deequ.suggestions.ConstraintSuggestion

/** If we see only numbers in a column, we suggest a corresponding maximum
  * constraint
  */
case class HasRowCount() extends ConstraintRule[ColumnProfile] {

  def rowCount(profile: ColumnProfile): Long = {
    profile.typeCounts.values.foldLeft(0L)(_ + _)
  }

  override def shouldBeApplied(
      profile: ColumnProfile,
      numRecords: Long
  ): Boolean = {
    rowCount(profile) > 0
  }

  override def candidate(
      profile: ColumnProfile,
      numRecords: Long
  ): ConstraintSuggestion = {

    val typeCounts: Map[String, Long] = profile.typeCounts
    val size: Long = rowCount(profile)

    val column = profile.column
    val description =
      s"""define a constraint on the number of rows""".stripMargin
        .replaceAll("\n", "")

    val hint =
      s"""count('$profile.column') should be == $size""".stripMargin
        .replaceAll("\n", "")

    val constraint = sizeConstraint(
      assertion = _ == size,
      where = None,
      hint = Some(hint)
    )

    ConstraintSuggestion(
      constraint,
      profile.column,
      hint,
      description,
      this,
      s"""SizeConstraint(_ == size, None, Some(hint))""".stripMargin
        .replaceAll("\n", "")
    )
  }

  override val ruleDescription: String =
    "define a constraint on the number of rows"
}
