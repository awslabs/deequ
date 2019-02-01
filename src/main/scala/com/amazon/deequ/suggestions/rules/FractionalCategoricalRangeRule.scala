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
import com.amazon.deequ.profiles.ColumnProfile
import com.amazon.deequ.suggestions.ConstraintSuggestion
import org.apache.commons.lang3.StringEscapeUtils
import scala.math.BigDecimal.RoundingMode

/** If we see a categorical range for most values in a column, we suggest an IS IN (...)
  * constraint that should hold for most values */
case class FractionalCategoricalRangeRule(targetDataCoverageFraction: Double = 0.9)
  extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    val hasHistogram = profile.histogram.isDefined && profile.dataType == DataTypeInstances.String

    if (hasHistogram) {
      val entries = profile.histogram.get.values

      val numUniqueElements = entries.count { case (_, value) => value.absolute == 1L }

      val uniqueValueRatio = numUniqueElements.toDouble / entries.size

      val topCategories = getTopCategoriesForFractionalDataCoverage(profile,
        targetDataCoverageFraction)
      val ratioSums = topCategories.map { case (_, value) => value.ratio }.sum

      // TODO find a principled way to define these thresholds...
      uniqueValueRatio <= 0.4 && ratioSums < 1
    } else {
      false
    }
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): ConstraintSuggestion = {

    val topCategories = getTopCategoriesForFractionalDataCoverage(profile,
      targetDataCoverageFraction)
    val ratioSums = topCategories.map { case (_, categoryValue) => categoryValue.ratio }.sum

    val valuesByPopularity = topCategories.toArray
      .filterNot { case (key, _) => key == Histogram.NullFieldReplacement }
      .sortBy { case (_, value) => value.absolute }
      .reverse

    val categoriesSql = valuesByPopularity
      // the character "'" can be contained in category names
      .map { case (key, _) => key.replace("'", "''") }
      .mkString("'", "', '", "'")

    val categoriesCode = valuesByPopularity
      .map { case (key, _) => StringEscapeUtils.escapeJava(key) }
      .mkString(""""""", """", """", """"""")

    val p = ratioSums
    val n = numRecords
    val z = 1.96

    // TODO this needs to be more robust for p's close to 0 or 1
    val targetCompliance = BigDecimal(p - z * math.sqrt(p * (1 - p) / n))
      .setScale(2, RoundingMode.DOWN).toDouble

    val description = s"'${profile.column}' has value range $categoriesSql for at least " +
      s"${targetCompliance * 100}% of values"
    val columnCondition = s"`${profile.column}` IN ($categoriesSql)"
    val hint = s"It should be above $targetCompliance!"
    val constraint = complianceConstraint(description, columnCondition, _ >= targetCompliance,
      hint = Some(hint))

    ConstraintSuggestion(
      constraint,
      profile.column,
      "Compliance: " + ratioSums.toString,
      description,
      this,
      s""".isContainedIn("${profile.column}", Array($categoriesCode),
         | _ >= $targetCompliance, Some("$hint"))""".stripMargin.replaceAll("\n", "")
    )
  }

  private[this] def getTopCategoriesForFractionalDataCoverage(
      columnProfile: ColumnProfile,
      dataCoverageFraction: Double)
    : Map[String, DistributionValue] = {

    val sortedHistogramValues = columnProfile.histogram.get.values.toSeq
      .sortBy { case (_, value) => value.ratio }.reverse

    var currentDataCoverage = 0.0
    var rangeValues = Map.empty[String, DistributionValue]

    sortedHistogramValues.foreach { case (categoryName, value) =>
      if (currentDataCoverage < dataCoverageFraction) {
        currentDataCoverage += value.ratio
        rangeValues += (categoryName -> value)
      }
    }

    rangeValues
  }

  override val ruleDescription: String = "If we see a categorical range for most values " +
    "in a column, we suggest an IS IN (...) constraint that should hold for most values"
}
