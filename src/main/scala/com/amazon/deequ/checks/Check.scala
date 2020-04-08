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

package com.amazon.deequ.checks

import com.amazon.deequ.anomalydetection.{AnomalyDetectionStrategy, AnomalyDetector, DataPoint}
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.analyzers.{Analyzer, Histogram, Patterns, State, KLLParameters}
import com.amazon.deequ.constraints.Constraint._
import com.amazon.deequ.constraints._
import com.amazon.deequ.metrics.{BucketDistribution, Distribution, Metric}
import com.amazon.deequ.repository.MetricsRepository
import org.apache.spark.sql.expressions.UserDefinedFunction
import com.amazon.deequ.anomalydetection.HistoryUtils
import com.amazon.deequ.checks.ColumnCondition.{isEachNotNull, isAnyNotNull}

import scala.util.matching.Regex

object CheckLevel extends Enumeration {
  val Error, Warning = Value
}

object CheckStatus extends Enumeration {
  val Success, Warning, Error = Value
}


case class CheckResult(
    check: Check,
    status: CheckStatus.Value,
    constraintResults: Seq[ConstraintResult])


/**
  * A class representing a list of constraints that can be applied to a given
  * [[org.apache.spark.sql.DataFrame]]. In order to run the checks, use the `run` method. You can
  * also use VerificationSuite.run to run your checks along with other Checks and Analysis objects.
  * When run with VerificationSuite, Analyzers required by multiple checks/analysis blocks is
  * optimized to run once.
  *
  * @param level           Assertion level of the check group. If any of the constraints fail this
  *                        level is used for the status of the check.
  * @param description     The name describes the check block. Generally will be used to show in
  *                        the logs.
  * @param constraints     The constraints to apply when this check is run. New ones can be added
  *                        and will return a new object
  */
case class Check(
  level: CheckLevel.Value,
  description: String,
  private[deequ] val constraints: Seq[Constraint] = Seq.empty) {


  /**
    * Returns a new Check object with the given constraint added to the constraints list.
    *
    * @param constraint New constraint to be added
    * @return
    */
  def addConstraint(constraint: Constraint): Check = {
    Check(level, description, constraints :+ constraint)
  }

  /** Adds a constraint that can subsequently be replaced with a filtered version */
  private[this] def addFilterableConstraint(
      creationFunc: Option[String] => Constraint)
    : CheckWithLastConstraintFilterable = {

    val constraintWithoutFiltering = creationFunc(None)

    CheckWithLastConstraintFilterable(level, description,
      constraints :+ constraintWithoutFiltering, creationFunc)
  }

  /**
    * Creates a constraint that calculates the data frame size and runs the assertion on it.
    *
    * @param assertion Function that receives a long input parameter and returns a boolean
    *                  Assertion functions might refer to the data frame size by "_"
    *                  .hasSize(_>5), meaning the number of rows should be greater than 5
    *                  Or more elaborate function might be provided
    *                  .hasSize{ aNameForSize => aNameForSize > 0 && aNameForSize < 10 }
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasSize(assertion: Long => Boolean, hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => Constraint.sizeConstraint(assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on a column completion.
    *
    * @param column Column to run the assertion on
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isComplete(column: String, hint: Option[String] = None): CheckWithLastConstraintFilterable = {
    addFilterableConstraint { filter => completenessConstraint(column, Check.IsOne, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on a column completion.
    * Uses the given history selection strategy to retrieve historical completeness values on this
    * column from the history provider.
    *
    * @param column    Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasCompleteness(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {
    addFilterableConstraint { filter => completenessConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on completion in combined set of columns.
    *
    * @param columns Columns to run the assertion on
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def areComplete(
      columns: Seq[String],
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {
    satisfies(isEachNotNull(columns), "Combined Completeness", Check.IsOne, hint)
  }

  /**
    * Creates a constraint that assert on completion in combined set of columns.
    *
    * @param columns Columns to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def haveCompleteness(
      columns: Seq[String],
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {
    satisfies(isEachNotNull(columns), "Combined Completeness", assertion, hint)
  }

  /**
   * Creates a constraint that asserts on completion in combined set of columns.
   *
   * @param columns Columns to run the assertion on
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def areAnyComplete(
      columns: Seq[String],
      hint: Option[String] = None)
  : CheckWithLastConstraintFilterable = {
    satisfies(isAnyNotNull(columns), "Any Completeness", Check.IsOne, hint)
  }

  /**
   * Creates a constraint that assert on completion in combined set of columns.
   *
   * @param columns Columns to run the assertion on
   * @param assertion Function that receives a double input parameter and returns a boolean
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def haveAnyCompleteness(
      columns: Seq[String],
      assertion: Double => Boolean,
      hint: Option[String] = None)
  : CheckWithLastConstraintFilterable = {
    satisfies(isAnyNotNull(columns), "Any Completeness", assertion, hint)
  }

  /**
    * Creates a constraint that asserts on a column uniqueness.
    *
    * @param column Column to run the assertion on
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isUnique(column: String, hint: Option[String] = None): CheckWithLastConstraintFilterable = {
    addFilterableConstraint { filter =>
      uniquenessConstraint(Seq(column), Check.IsOne, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on a column(s) primary key characteristics.
    * Currently only checks uniqueness, but reserved for primary key checks if there is another
    * assertion to run on primary key columns.
    *
    * @param column Columns to run the assertion on
    * @return
    */
  def isPrimaryKey(column: String, columns: String*): CheckWithLastConstraintFilterable = {
    addFilterableConstraint { filter =>
      uniquenessConstraint(column :: columns.toList, Check.IsOne, filter) }
  }

  /**
    * Creates a constraint that asserts on a column(s) primary key characteristics.
    * Currently only checks uniqueness, but reserved for primary key checks if there is another
    * assertion to run on primary key columns.
    *
    * @param column Columns to run the assertion on
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isPrimaryKey(column: String, hint: Option[String], columns: String*)
    : CheckWithLastConstraintFilterable = {
    addFilterableConstraint { filter =>
      uniquenessConstraint(column :: columns.toList, Check.IsOne, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on uniqueness in a single or combined set of key columns.
    *
    * @param columns Key columns
    * @param assertion Function that receives a double input parameter and returns a boolean.
    *                  Refers to the fraction of unique values
    * @return
    */
  def hasUniqueness(columns: Seq[String], assertion: Double => Boolean)
    : CheckWithLastConstraintFilterable = {
    addFilterableConstraint { filter => uniquenessConstraint(columns, assertion, filter) }
  }

  /**
    * Creates a constraint that asserts on uniqueness in a single or combined set of key columns.
    *
    * @param columns Key columns
    * @param assertion Function that receives a double input parameter and returns a boolean.
    *                  Refers to the fraction of unique values
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasUniqueness(
      columns: Seq[String],
      assertion: Double => Boolean,
      hint: Option[String])
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => uniquenessConstraint(columns, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the uniqueness of a key column.
    *
    * @param column Key column
    * @param assertion Function that receives a double input parameter and returns a boolean.
    *                  Refers to the fraction of unique values.
    * @return
    */
  def hasUniqueness(column: String, assertion: Double => Boolean)
    : CheckWithLastConstraintFilterable = {
    hasUniqueness(Seq(column), assertion)
  }

  /**
    * Creates a constraint that asserts on the uniqueness of a key column.
    *
    * @param column Key column
    * @param assertion Function that receives a double input parameter and returns a boolean.
    *                  Refers to the fraction of unique values.
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasUniqueness(column: String, assertion: Double => Boolean, hint: Option[String])
    : CheckWithLastConstraintFilterable = {
    hasUniqueness(Seq(column), assertion, hint)
  }

  /**
    * Creates a constraint on the distinctness in a single or combined set of key columns.
    *
    * @param columns columns
    * @param assertion Function that receives a double input parameter and returns a boolean.
    *                  Refers to the fraction of distinct values.
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasDistinctness(
      columns: Seq[String], assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => distinctnessConstraint(columns, assertion, filter, hint) }
  }

  /**
    * Creates a constraint on the unique value ratio in a single or combined set of key columns.
    *
    * @param columns columns
    * @param assertion Function that receives a double input parameter and returns a boolean.
    *                  Refers to the fraction of distinct values.
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasUniqueValueRatio(
      columns: Seq[String],
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      uniqueValueRatioConstraint(columns, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the number of distinct values a column has.
    *
    * @param column     Column to run the assertion on
    * @param assertion  Function that receives a long input parameter and returns a boolean
    * @param binningUdf An optional binning function
    * @param maxBins    Histogram details is only provided for N column values with top counts.
    *                   maxBins sets the N
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasNumberOfDistinctValues(
      column: String,
      assertion: Long => Boolean,
      binningUdf: Option[UserDefinedFunction] = None,
      maxBins: Integer = Histogram.MaximumAllowedDetailBins,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      histogramBinConstraint(column, assertion, binningUdf, maxBins, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on column's value distribution.
    *
    * @param column     Column to run the assertion on
    * @param assertion  Function that receives a Distribution input parameter and returns a boolean.
    *                   E.g
    *                   .hasHistogramValues("att2", _.absolutes("f") == 3)
    *                   .hasHistogramValues("att2",
    *                   _.ratios(Histogram.NullFieldReplacement) == 2/6.0)
    * @param binningUdf An optional binning function
    * @param maxBins    Histogram details is only provided for N column values with top counts.
    *                   maxBins sets the N
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasHistogramValues(
      column: String,
      assertion: Distribution => Boolean,
      binningUdf: Option[UserDefinedFunction] = None,
      maxBins: Integer = Histogram.MaximumAllowedDetailBins,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      histogramConstraint(column, assertion, binningUdf, maxBins, filter, hint) }
  }

  /**
   * Creates a constraint that asserts on column's sketch size.
   *
   * @param column    Column to run the assertion on
   * @param assertion Function that receives a Distribution input parameter and returns a boolean.
   *                  E.g
   *                  .hasLargeKLLSketchSize("att2", _.parameters(1) >= 16,
   *                  kllParameters = Option(kllParameters(2, 0.64, 2)))
   * @param kllParameters parameters of KLL Sketch
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def kllSketchSatisfies(
                          column: String,
                          assertion: BucketDistribution => Boolean,
                          kllParameters: Option[KLLParameters] = None,
                          hint: Option[String] = None)
    : Check = {

    addConstraint(kllConstraint(column, assertion, kllParameters, hint))
  }

  /**
    * Creates a constraint that runs AnomalyDetection on the new value
    *
    * @param metricsRepository        A metrics repository to get the previous results
    * @param anomalyDetectionStrategy The anomaly detection strategy
    * @param analyzer                 The analyzer for the metric to run anomaly detection on
    * @param withTagValues            Can contain a Map with tag names and the corresponding values
    *                                 to filter for
    * @param beforeDate               The maximum dateTime of previous AnalysisResults to use for
    *                                 the Anomaly Detection
    * @param afterDate                The minimum dateTime of previous AnalysisResults to use for
    *                                 the Anomaly Detection
    * @param hint                     A hint to provide additional context why a constraint
    *                                 could have failed
    * @return
    */
  private[deequ] def isNewestPointNonAnomalous[S <: State[S]](
      metricsRepository: MetricsRepository,
      anomalyDetectionStrategy: AnomalyDetectionStrategy,
      analyzer: Analyzer[S, Metric[Double]],
      withTagValues: Map[String, String],
      afterDate: Option[Long],
      beforeDate: Option[Long],
      hint: Option[String] = None)
    : Check = {

    val anomalyAssertionFunction = Check.isNewestPointNonAnomalous(
      metricsRepository,
      anomalyDetectionStrategy,
      analyzer,
      withTagValues,
      afterDate,
      beforeDate
    )(_)

    addConstraint(anomalyConstraint(analyzer, anomalyAssertionFunction, hint))
  }


  /**
    * Creates a constraint that asserts on a column entropy.
    *
    * @param column    Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint      A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasEntropy(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => entropyConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on a mutual information between two columns.
    *
    * @param columnA   First column for mutual information calculation
    * @param columnB   Second column for mutual information calculation
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint      A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasMutualInformation(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      mutualInformationConstraint(columnA, columnB, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on an approximated quantile
    *
    * @param column Column to run the assertion on
    * @param quantile Which quantile to assert on
    * @param assertion Function that receives a double input parameter (the computed quantile)
    *                  and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasApproxQuantile(column: String,
      quantile: Double,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint( filter =>
      approxQuantileConstraint(column, quantile, assertion, filter, hint))
  }

  /**
    * Creates a constraint that asserts on the minimum length of the column
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasMinLength(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => minLengthConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the maximum length of the column
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasMaxLength(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => maxLengthConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the minimum of the column
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasMin(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => minConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the maximum of the column
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasMax(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => maxConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the mean of the column
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasMean(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => meanConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the sum of the column
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasSum(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter => sumConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the standard deviation of the column
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasStandardDeviation(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      standardDeviationConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the approximate count distinct of the given column
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasApproxCountDistinct(
      column: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      approxCountDistinctConstraint(column, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts on the pearson correlation between two columns.
    *
    * @param columnA   First column for correlation calculation
    * @param columnB   Second column for correlation calculation
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasCorrelation(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      correlationConstraint(columnA, columnB, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that runs the given condition on the data frame.
    *
    * @param columnCondition Data frame column which is a combination of expression and the column
    *                        name. It has to comply with Spark SQL syntax.
    *                        Can be written in an exact same way with conditions inside the
    *                        `WHERE` clause.
    * @param constraintName  A name that summarizes the check being made. This name is being used to
    *                        name the metrics for the analysis being done.
    * @param assertion       Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def satisfies(
      columnCondition: String,
      constraintName: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      complianceConstraint(constraintName, columnCondition, assertion, filter, hint)
    }
  }

  /**
    * Checks for pattern compliance. Given a column name and a regular expression, defines a
    * Check on the average compliance of the column's values to the regular expression.
    *
    * @param column Name of the column that should be checked.
    * @param pattern The columns values will be checked for a match against this pattern.
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasPattern(
      column: String,
      pattern: Regex,
      assertion: Double => Boolean = Check.IsOne,
      name: Option[String] = None,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      Constraint.patternMatchConstraint(column, pattern, assertion, filter, name, hint)
    }
  }

  /**
    * Check to run against the compliance of a column against a Credit Card pattern.
    *
    * @param column Name of the column that should be checked.
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def containsCreditCardNumber(
      column: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    hasPattern(column, Patterns.CREDITCARD, assertion, Some(s"containsCreditCardNumber($column)"),
      hint)
  }

  /**
    * Check to run against the compliance of a column against an e-mail pattern.
    *
    * @param column Name of the column that should be checked.
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def containsEmail(
      column: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    hasPattern(column, Patterns.EMAIL, assertion, Some(s"containsEmail($column)"), hint)
  }

  /**
    * Check to run against the compliance of a column against an URL pattern.
    *
    * @param column Name of the column that should be checked.
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def containsURL(
      column: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    hasPattern(column, Patterns.URL, assertion, Some(s"containsURL($column)"), hint)
  }

  /**
    * Check to run against the compliance of a column against the Social security number pattern
    * for the US.
    *
    * @param column Name of the column that should be checked.
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def containsSocialSecurityNumber(
      column: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    hasPattern(column, Patterns.SOCIAL_SECURITY_NUMBER_US, assertion,
      Some(s"containsSocialSecurityNumber($column)"), hint)
  }

  /**
    * Check to run against the fraction of rows that conform to the given data type.
    *
    * @param column Name of the columns that should be checked.
    * @param dataType Data type that the columns should be compared against.
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def hasDataType(
      column: String,
      dataType: ConstrainableDataTypes.Value,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    addFilterableConstraint { filter =>
      Constraint.dataTypeConstraint(column, dataType, assertion, filter, hint) }
  }

  /**
    * Creates a constraint that asserts that a column contains no negative values
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isNonNegative(
      column: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    // coalescing column to not count NULL values as non-compliant
    satisfies(s"COALESCE($column, 0.0) >= 0", s"$column is non-negative", assertion, hint = hint)
  }

  /**
    * Creates a constraint that asserts that a column contains no negative values
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isPositive(
      column: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {
    // coalescing column to not count NULL values as non-compliant
    satisfies(s"COALESCE($column, 1.0) > 0", s"$column is positive", assertion, hint)
  }

  /**
    *
    * Asserts that, in each row, the value of columnA is less than the value of columnB
    *
    * @param columnA Column to run the assertion on
    * @param columnB Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isLessThan(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    satisfies(s"$columnA < $columnB", s"$columnA is less than $columnB", assertion,
      hint = hint)
  }

  /**
    * Asserts that, in each row, the value of columnA is less than or equal to the value of columnB
    *
    * @param columnA Column to run the assertion on
    * @param columnB Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isLessThanOrEqualTo(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    satisfies(s"$columnA <= $columnB", s"$columnA is less than or equal to $columnB",
      assertion, hint = hint)
  }

  /**
    * Asserts that, in each row, the value of columnA is greater than the value of columnB
    *
    * @param columnA Column to run the assertion on
    * @param columnB Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isGreaterThan(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    satisfies(s"$columnA > $columnB", s"$columnA is greater than $columnB", assertion,
      hint = hint)
  }

  /**
    * Asserts that, in each row, the value of columnA is greather than or equal to the value of
    * columnB
    *
    * @param columnA Column to run the assertion on
    * @param columnB Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isGreaterThanOrEqualTo(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean = Check.IsOne,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    satisfies(s"$columnA >= $columnB", s"$columnA is greater than or equal to $columnB",
      assertion, hint = hint)
  }

  // We can't use default values here as you can't combine default values and overloading in Scala
  /**
    * Asserts that every non-null value in a column is contained in a set of predefined values
    *
    * @param column Column to run the assertion on
    * @param allowedValues allowed values for the column
    * @return
    */
  def isContainedIn(
      column: String,
      allowedValues: Array[String])
    : CheckWithLastConstraintFilterable = {


    isContainedIn(column, allowedValues, Check.IsOne, None)
  }

  // We can't use default values here as you can't combine default values and overloading in Scala
  /**
    * Asserts that every non-null value in a column is contained in a set of predefined values
    *
    * @param column Column to run the assertion on
    * @param allowedValues allowed values for the column
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isContainedIn(
      column: String,
      allowedValues: Array[String],
      hint: Option[String])
    : CheckWithLastConstraintFilterable = {

    isContainedIn(column, allowedValues, Check.IsOne, hint)
  }

  // We can't use default values here as you can't combine default values and overloading in Scala
  /**
    * Asserts that every non-null value in a column is contained in a set of predefined values
    *
    * @param column Column to run the assertion on
    * @param allowedValues Allowed values for the column
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @return
    */
  def isContainedIn(
      column: String,
      allowedValues: Array[String],
      assertion: Double => Boolean)
    : CheckWithLastConstraintFilterable = {


    isContainedIn(column, allowedValues, assertion, None)
  }

  // We can't use default values here as you can't combine default values and overloading in Scala
  /**
    * Asserts that every non-null value in a column is contained in a set of predefined values
    *
    * @param column Column to run the assertion on
    * @param allowedValues Allowed values for the column
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isContainedIn(
      column: String,
      allowedValues: Array[String],
      assertion: Double => Boolean,
      hint: Option[String])
    : CheckWithLastConstraintFilterable = {


    val valueList = allowedValues
      .map { _.replaceAll("'", "''") }
      .mkString("'", "','", "'")

    val predicate = s"`$column` IS NULL OR `$column` IN ($valueList)"
    satisfies(predicate, s"$column contained in ${allowedValues.mkString(",")}", assertion, hint)
  }

  /**
    * Asserts that the non-null values in a numeric column fall into the predefined interval
    *
    * @param column column to run the assertion
    * @param lowerBound lower bound of the interval
    * @param upperBound upper bound of the interval
    * @param includeLowerBound is a value equal to the lower bound allows?
    * @param includeUpperBound is a value equal to the upper bound allowed?
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def isContainedIn(
      column: String,
      lowerBound: Double,
      upperBound: Double,
      includeLowerBound: Boolean = true,
      includeUpperBound: Boolean = true,
      hint: Option[String] = None)
    : CheckWithLastConstraintFilterable = {

    val leftOperand = if (includeLowerBound) ">=" else ">"
    val rightOperand = if (includeUpperBound) "<=" else "<"

    val predicate = s"`$column` IS NULL OR " +
      s"(`$column` $leftOperand $lowerBound AND `$column` $rightOperand $upperBound)"

    satisfies(predicate, s"$column between $lowerBound and $upperBound", hint = hint)
  }

  /**
    * Evaluate this check on computed metrics
    * @param context result of the metrics computation
    * @return
    */
  def evaluate(context: AnalyzerContext): CheckResult = {

    val constraintResults = constraints.map { _.evaluate(context.metricMap) }
    val anyFailures = constraintResults.exists { _.status == ConstraintStatus.Failure }

    val checkStatus = (anyFailures, level) match {
      case (true, CheckLevel.Error) => CheckStatus.Error
      case (true, CheckLevel.Warning) => CheckStatus.Warning
      case (_, _) => CheckStatus.Success
    }

    CheckResult(this, checkStatus, constraintResults)
  }

  def requiredAnalyzers(): Set[Analyzer[_, Metric[_]]] = {
    constraints
      .map {
        case nc: ConstraintDecorator => nc.inner
        case c: Constraint => c
      }
      .collect { case constraint: AnalysisBasedConstraint[_, _, _] => constraint.analyzer }
      .map { _.asInstanceOf[Analyzer[_, Metric[_]]] }
      .toSet
  }
}

object Check {

  /** A common assertion function checking if the value is 1 */
  val IsOne: Double => Boolean = { _ == 1.0 }

  /**
    * Common assertion function checking if the value can be considered as normal (that no
    * anomalies were detected), given the anomaly detection strategy and details on how to retrieve
    * the history
    *
    * @param metricsRepository        A metrics repository to get the previous results
    * @param anomalyDetectionStrategy The anomaly detection strategy
    * @param analyzer                 The analyzer for the metric to run anomaly detection on
    * @param withTagValues            Can contain a Map with tag names and the corresponding values
    *                                 to filter for
    * @param beforeDate               The maximum dateTime of previous AnalysisResults to use for
    *                                 the Anomaly Detection
    * @param afterDate                The minimum dateTime of previous AnalysisResults to use for
    *                                 the Anomaly Detection
    * @param currentMetricValue       current metric value
    * @return
    */
  private[deequ] def isNewestPointNonAnomalous[S <: State[S]](
      metricsRepository: MetricsRepository,
      anomalyDetectionStrategy: AnomalyDetectionStrategy,
      analyzer: Analyzer[S, Metric[Double]],
      withTagValues: Map[String, String],
      afterDate: Option[Long],
      beforeDate: Option[Long])(
      currentMetricValue: Double)
    : Boolean = {

    // Get history keys
    var repositoryLoader = metricsRepository.load()

    repositoryLoader = repositoryLoader.withTagValues(withTagValues)

    beforeDate.foreach { beforeDate =>
      repositoryLoader = repositoryLoader.before(beforeDate) }

    afterDate.foreach { afterDate =>
      repositoryLoader = repositoryLoader.after(afterDate) }

    repositoryLoader = repositoryLoader.forAnalyzers(Seq(analyzer))

    val analysisResults = repositoryLoader.get()

    require(analysisResults.nonEmpty, "There have to be previous results in the MetricsRepository!")

    val historicalMetrics = analysisResults
      // If we have multiple DataPoints with the same dateTime, which should not happen in most
      // cases, we still want consistent behaviour, so we sort them by Tags first
      // (sorting is stable in Scala)
      .sortBy(_.resultKey.tags.values)
      .map { analysisResult =>
        val analyzerContextMetricMap = analysisResult.analyzerContext.metricMap

        val onlyAnalyzerMetricEntryInLoadedAnalyzerContext = analyzerContextMetricMap.headOption

        val doubleMetricOption = onlyAnalyzerMetricEntryInLoadedAnalyzerContext
          .collect { case (_, metric) => metric.asInstanceOf[Metric[Double]] }

        val dataSetDate = analysisResult.resultKey.dataSetDate

        (dataSetDate, doubleMetricOption)
      }

    // Ensure this is the last dataPoint
    val testDateTime = analysisResults.map(_.resultKey.dataSetDate).max + 1
    require(testDateTime != Long.MaxValue, "Test DateTime cannot be Long.MaxValue, otherwise the" +
        "Anomaly Detection, which works with an open upper interval bound, won't test anything")

    // Run given anomaly detection strategy and return false if the newest value is an Anomaly
    val anomalyDetector = AnomalyDetector(anomalyDetectionStrategy)
    val detectedAnomalies = anomalyDetector.isNewPointAnomalous(
      HistoryUtils.extractMetricValues[Double](historicalMetrics),
      DataPoint(testDateTime, Some(currentMetricValue)))

    detectedAnomalies.anomalies.isEmpty
  }
}
