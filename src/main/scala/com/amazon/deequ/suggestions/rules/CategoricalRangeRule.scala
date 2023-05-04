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

import com.amazon.deequ.analyzers.{DataTypeInstances, Histogram}
import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.Constraint.complianceConstraint
import com.amazon.deequ.profiles.ColumnProfile
import com.amazon.deequ.suggestions.ConstraintSuggestion
import org.apache.commons.lang3.StringEscapeUtils
import com.amazon.deequ.metrics.DistributionValue

/** If we see a categorical range for a column, we suggest an IS IN (...) constraint */
case class CategoricalRangeRule(
  categorySorter: Array[(String, DistributionValue)] => Array[(String, DistributionValue)] =
    categories => categories.sortBy({ case (_, value) => value.absolute }).reverse
) extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    val hasHistogram = profile.histogram.isDefined && (
      profile.dataType == DataTypeInstances.String ||
      profile.dataType == DataTypeInstances.Integral
    )

    if (hasHistogram) {
      val entries = profile.histogram.get.values

      val numUniqueElements = entries.count { case (_, value) => value.absolute == 1L }

      val uniqueValueRatio = numUniqueElements.toDouble / entries.size

      // TODO find a principled way to define this threshold...
      uniqueValueRatio <= 0.1
    } else {
      false
    }
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): ConstraintSuggestion = {
    val valuesByPopularityNotNull = profile.histogram.get.values.toArray
      .filterNot { case (key, _) => key == Histogram.NullFieldReplacement }
    val valuesByPopularity = categorySorter(valuesByPopularityNotNull)

    val categoriesSql = valuesByPopularity
      // the character "'" can be contained in category names
      .map { case (key, _) => key.replace("'", "''") }
      .mkString("'", "', '", "'")

    val categoriesCode = valuesByPopularity
      .map { case (key, _) => StringEscapeUtils.escapeJava(key) }
      .mkString(""""""", """", """", """"""")

    val description = s"'${profile.column}' has value range $categoriesSql"
    val columnCondition = s"`${profile.column}` IN ($categoriesSql)"
    val constraint = complianceConstraint(description, columnCondition, List(profile.column), Check.IsOne)

    ConstraintSuggestion(
      constraint,
      profile.column,
      "Compliance: 1",
      description,
      this,
      s""".isContainedIn("${profile.column}", Array($categoriesCode))"""
    )
  }

  override val ruleDescription: String = "If we see a categorical range for a " +
    "column, we suggest an IS IN (...) constraint"
}
