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
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.analyzers.DataTypeInstances._
import com.amazon.deequ.constraints.Constraint.{dataTypeConstraint}
import com.amazon.deequ.profiles.{ColumnProfile, NumericColumnProfile}
import com.amazon.deequ.suggestions.ConstraintSuggestion
import com.amazon.deequ.analyzers.DataTypeInstances

/** If we see only numbers in a column, we suggest a corresponding maximum
  * constraint
  */
case class NumberHasNumericDataType() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(
      profile: ColumnProfile,
      numRecords: Long
  ): Boolean = {
    getConstraintDataType(profile) match {
      case Some(dataType) => true // scalastyle:off
      case _              => false // scalastyle:off
    }
  }

  def getConstraintDataType(
      profile: ColumnProfile
  ): Option[ConstrainableDataTypes.Value] = {
    val datatypeMapping
        : Map[DataTypeInstances.Value, Option[ConstrainableDataTypes.Value]] =
      Map(
        (DataTypeInstances.Fractional -> Some(
          ConstrainableDataTypes.Fractional
        )),
        (DataTypeInstances.Integral -> Some(ConstrainableDataTypes.Integral))
      )
    datatypeMapping.getOrElse(profile.dataType, None)
  }

  override def candidate(
      profile: ColumnProfile,
      numRecords: Long
  ): ConstraintSuggestion = {

    val dataType: Option[ConstrainableDataTypes.Value] = getConstraintDataType(
      profile
    )
    val dataTypeValue = dataType.get
    val column = profile.column
    val description =
      s"""'$column' datatype should be "${dataTypeValue}"
        s"[minimum, maximum]""".stripMargin.replaceAll("\n", "")

    val hint = s"${profile.column} non-null values are all numeric"

    val constraint = dataTypeConstraint(
      column = column,
      dataType = dataTypeValue,
      assertion = _ == 1.0,
      hint = Some(hint)
    )

    ConstraintSuggestion(
      constraint,
      profile.column,
      s"""'$column' non null values should be of Numerical type""",
      description,
      this,
      s""".hasDataType("$column", $dataTypeValue)""".stripMargin
        .replaceAll("\n", "")
    )
  }

  override val ruleDescription: String = "If we see numbers in a " +
    "column, we suggest a corresponding datatype constraint"
}
