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
import com.amazon.deequ.constraints.Constraint.complianceConstraint
import com.amazon.deequ.metrics.DistributionValue
import com.amazon.deequ.suggestions.{ColumnProfile, ConstraintSuggestion}
import scala.math.BigDecimal.RoundingMode

/** If we see a categorical range for most values in a column, we suggest an IS IN (...)
  * constraint that should hold for most values */
object FractionalCategoricalRangeRule extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    val hasHistogram = profile.histogram.isDefined && profile.dataType == DataTypeInstances.String

    if (hasHistogram) {
      val entries = profile.histogram.get.values

      val numUniqueElements = entries.count { case (_, value) => value.absolute == 1L }

      val uniqueValueRatio = numUniqueElements.toDouble / entries.size

      val sortedHistogramValues = profile.histogram.get.values.toSeq
        .sortBy { case (_, value) => value.ratio }.reverse

      var currentDataCoverage = 0.0
      var rangeValues = Seq.empty[DistributionValue]

      sortedHistogramValues.foreach { case (_, value) =>
        if (currentDataCoverage < 0.90) {
          currentDataCoverage += value.ratio
          rangeValues :+= value
        }
      }
      val ratioSums = rangeValues.map { value => value.ratio }.sum

      // TODO find a principled way to define these thresholds...
      uniqueValueRatio <= 0.4 && ratioSums < 1
    } else {
      false
    }
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): ConstraintSuggestion = {

    val sortedHistogramValues = profile.histogram.get.values.toSeq
      .sortBy { case (_, value) => value.ratio }.reverse

    var currentDataCoverage = 0.0
    var rangeValues = Map.empty[String, DistributionValue]

    sortedHistogramValues.foreach { case (categoryName, value) =>
      if (currentDataCoverage < 0.90) {
        currentDataCoverage += value.ratio
        rangeValues += (categoryName -> value)
      }
    }
    val ratioSums = rangeValues.map { _._2.ratio }.sum

    val valuesByPopularity = rangeValues.toArray
      .filterNot { case (key, _) => key == Histogram.NullFieldReplacement }
      .sortBy { case (_, value) => value.absolute }
      .reverse
      // the character "'" can be contained in category names
      .map { case (key, _) => key.replace("'", "''") }
      .mkString("'", "', '", "'")

    val p = ratioSums
    val n = numRecords
    val z = 1.96

    // TODO this needs to be more robust for p's close to 0 or 1
    val targetCompliance = BigDecimal(p - z * math.sqrt(p * (1 - p) / n))
      .setScale(2, RoundingMode.DOWN).toDouble

    val description = s"'${profile.column}' has value range $valuesByPopularity for at least " +
      s"${targetCompliance * 100}% of values"
    val columnCondition = s"${profile.column} IN ($valuesByPopularity)"
    val constraint = complianceConstraint(description, columnCondition, _ >= targetCompliance)

    ConstraintSuggestion(
      constraint,
      profile.column,
      "Compliance: " + ratioSums.toString,
      description,
      this,
      s""".isContainedIn("${profile.column}", Seq($valuesByPopularity),
         |_ >= $targetCompliance)""".stripMargin
    )
  }

  override val ruleDescription: String = "If we see a categorical range for most values " +
    "in a column, we suggest an IS IN (...) constraint that should hold for most values"
}
