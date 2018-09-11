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

import com.amazon.deequ.analyzers.{DataTypeInstances, Histogram}
import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.{ConstrainableDataTypes, Constraint, NamedConstraint}
import org.apache.spark.sql.functions.expr
import Constraint._

/** Abstract base class for all constraint suggestion rules */
abstract class ConstraintRule[P <: ColumnProfile] {

  /**
    * Decide whether the rule should be applied to a particular column
    *
    * @param profile  profile of the column
    * @param numRecords overall number of records
    * @return
    */
  def shouldBeApplied(profile: P, numRecords: Long): Boolean

  /**
    * Generated a suggested constraint for the column
    *
    * @param profile  profile of the column
    * @param numRecords overall number of records
    * @return
    */
  def candidate(profile: P, numRecords: Long): NamedConstraint
}

/** If a column is complete in the sample, we suggest a NOT NULL constraint */
object CompleteIfCompleteRule extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    profile.completeness == 1.0
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): NamedConstraint = {

    val constraint = completenessConstraint(profile.column, Check.IsOne)

    named(constraint, s"'${profile.column}' is not null")
  }
}

/**
  * If a column is incomplete in the sample, we model its completeness as a binomial variable,
  * estimate a confidence interval and use this to define a lower bound for the completeness
  */
object RetainCompletenessRule extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    profile.completeness < 1.0
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): NamedConstraint = {

    val p = profile.completeness
    val n = numRecords
    val z = 1.96

    // TODO this needs to be more robust for p's close to 0 or 1
    val targetCompleteness = p - z * math.sqrt(p * (1 - p) / n)

    val constraint = completenessConstraint(profile.column, _ >= targetCompleteness)

    val boundInPercent = ((1.0 - targetCompleteness) * 100).toInt

    named(constraint, s"'${profile.column}' has less than $boundInPercent% missing values")
  }
}

/**
  * If the ratio of approximate num distinct values in a column is close to the number of records
  * (within error of HLL sketch), we suggest a UNIQUE constraint
  */
object UniqueIfApproximatelyUniqueRule extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {

    val approximateDistinctness = profile.approximateNumDistinctValues.toDouble / numRecords

    // TODO This bound depends on the error guarantees of the HLL sketch
    profile.completeness == 1.0 && math.abs(1.0 - approximateDistinctness) <= 0.08
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): NamedConstraint = {

    val constraint = uniquenessConstraint(Seq(profile.column), Check.IsOne)

    named(constraint, s"'${profile.column}' is unique")
  }
}

/** If we detect a non-string type, we suggest a type constraint */
object RetainTypeRule extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    val testableType = profile.dataType match {
      case DataTypeInstances.Integral | DataTypeInstances.Fractional | DataTypeInstances.Boolean =>
        true
      case _ => false
    }

    profile.isDataTypeInferred && testableType
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): NamedConstraint = {

    val typeToCheck = profile.dataType match {
      case DataTypeInstances.Fractional => ConstrainableDataTypes.Fractional
      case DataTypeInstances.Integral => ConstrainableDataTypes.Integral
      case DataTypeInstances.Boolean => ConstrainableDataTypes.Boolean
    }

    val constraint = dataTypeConstraint(profile.column, typeToCheck, Check.IsOne)

    named(constraint, s"'${profile.column}' has type ${profile.dataType}")
  }
}

/** If we see a categorical range for a column, we suggest an IS IN (...) constraint */
object CategoricalRangeRule extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    val hasHistogram = profile.histogram.isDefined && profile.dataType == DataTypeInstances.String

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

  override def candidate(profile: ColumnProfile, numRecords: Long): NamedConstraint = {

    val valuesByPopularity = profile.histogram.get.values.toArray
      .filterNot { case (key, _) => key == Histogram.NullFieldReplacement }
      .sortBy { case (_, value) => value.absolute }
      .reverse
      .map { case (key, _) => key }
      .mkString("'", "', '", "'")

    val name = s"'${profile.column}' has value range $valuesByPopularity"
    val constraint = complianceConstraint(name, s"${profile.column} IN ($valuesByPopularity)",
      Check.IsOne)

    named(constraint, name)
  }
}

/** If we see only non-negative numbers in a column, we suggest a corresponding constraint */
object NonNegativeNumbersRule extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {
    profile match {
      case numericProfile: NumericColumnProfile
        if numericProfile.minimum.isDefined && numericProfile.minimum.get == 0.0 => true
      case _ => false
    }
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): NamedConstraint = {

    val name = s"'${profile.column}' has no negative values"
    val constraint = complianceConstraint(name, s"${profile.column} >= 0", Check.IsOne)

    named(constraint, name)
  }
}

/** If we see only positive numbers in a column, we suggest a corresponding constraint */
object PositiveNumbersRule extends ConstraintRule[ColumnProfile] {

  override def shouldBeApplied(profile: ColumnProfile, numRecords: Long): Boolean = {

    profile match {
      case numericProfile: NumericColumnProfile
        if numericProfile.minimum.isDefined && numericProfile.minimum.get > 0.0 => true
      case _ => false
    }
  }

  override def candidate(profile: ColumnProfile, numRecords: Long): NamedConstraint = {

    val name = s"'${profile.column}' has only positive values"
    val constraint = complianceConstraint(name, s"${profile.column} > 0", Check.IsOne)

    named(constraint, name)
  }
}


