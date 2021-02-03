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

import com.amazon.deequ.analyzers.DataTypeInstances
import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.constraints.Constraint.dataTypeConstraint
import com.amazon.deequ.profiles.ColumnProfile
import com.amazon.deequ.suggestions.ConstraintSuggestion

/** If we detect a non-string type, we suggest a type constraint */
case class RetainTypeRule() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    val testableType = profile.dataType match {
      case DataTypeInstances.Integral | DataTypeInstances.Fractional | DataTypeInstances.Boolean =>
        true
      case _ => false
    }

    profile.isDataTypeInferred && testableType
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): ConstraintSuggestion = {

    val typeToCheck = profile.dataType match {
      case DataTypeInstances.Fractional => ConstrainableDataTypes.Fractional
      case DataTypeInstances.Integral => ConstrainableDataTypes.Integral
      case DataTypeInstances.Boolean => ConstrainableDataTypes.Boolean
    }

    val description = s"'${profile.column}' has data type ${profile.dataType}"
    val constraint =
      dataTypeConstraint(profile.column, typeToCheck, Check.IsOne, hint = Some(description))
    val hintCode = ConstraintRule.genHintCode(description)

    ConstraintSuggestion(
      constraint,
      profile.column,
      "DataType: " + profile.dataType.toString,
      description,
      this,
      s""".hasDataType("${profile.column}", """ +
      s"""ConstrainableDataTypes.${profile.dataType}, """ +
      s"""hint = ${hintCode})"""
    )
  }

  override val ruleDescription: String = "If we detect a non-string type, we suggest a " +
    "type constraint"
}
