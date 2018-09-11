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

package com.amazon.deequ.suggestions


import com.amazon.deequ.constraints.NamedConstraint
import org.apache.spark.sql.DataFrame

case class SuggestedConstraints(columnProfiles: ColumnProfiles, constraints: Seq[NamedConstraint])

/**
  * Generate suggestions for constraints by applying the rules on the column profiles computed from
  * the data at hand.
  *
  */
object ConstraintSuggestions {

  private val RULES: Seq[ConstraintRule[ColumnProfile]] =
    Seq(CompleteIfCompleteRule, RetainCompletenessRule, UniqueIfApproximatelyUniqueRule,
      RetainTypeRule, CategoricalRangeRule, NonNegativeNumbersRule, PositiveNumbersRule)

  def suggest(trainingData: DataFrame,
    fromColumns: Option[Array[String]] = None): SuggestedConstraints = {

    val interestingColumns = fromColumns.getOrElse(trainingData.schema.fieldNames)

    val profiles = ColumnProfiler.profile(trainingData, interestingColumns)

    val constraints = applyRules(profiles, interestingColumns)

    SuggestedConstraints(profiles, constraints)
  }

  private[suggestions] def applyRules(
      profiles: ColumnProfiles,
      columns: Array[String])
    : Seq[NamedConstraint] = {

    columns
      .filter { name => profiles.profiles.contains(name) }
      .flatMap { column =>

        val profile = profiles.profiles(column)

        RULES
          .filter { _.shouldBeApplied(profile, profiles.numRecords) }
          .map { _.candidate(profile, profiles.numRecords) }
      }
  }

}
