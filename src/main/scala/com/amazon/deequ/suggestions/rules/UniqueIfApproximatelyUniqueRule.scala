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

import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.Constraint.uniquenessConstraint
import com.amazon.deequ.profiles.ColumnProfile
import com.amazon.deequ.suggestions.ConstraintSuggestion

/**
  * If the ratio of approximate num distinct values in a column is close to the number of records
  * (within error of HLL sketch), we suggest a UNIQUE constraint
  */
case class UniqueIfApproximatelyUniqueRule() extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {

    val approximateDistinctness = profile.approximateNumDistinctValues.toDouble / numRecords

    // TODO This bound depends on the error guarantees of the HLL sketch
    profile.completeness == 1.0 && math.abs(1.0 - approximateDistinctness) <= 0.08
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): ConstraintSuggestion = {

    val constraint = uniquenessConstraint(Seq(profile.column), Check.IsOne)
    val approximateDistinctness = profile.approximateNumDistinctValues.toDouble / numRecords

    ConstraintSuggestion(
      constraint,
      profile.column,
      "ApproxDistinctness: " + approximateDistinctness.toString,
      s"'${profile.column}' is unique",
      this,
      s""".isUnique("${profile.column}")"""
    )
  }

  override val ruleDescription: String = "If the ratio of approximate num distinct values " +
    "in a column is close to the number of records (within the error of the HLL sketch), " +
    "we suggest a UNIQUE constraint"
}
