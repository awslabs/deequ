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

package com.amazon.deequ.constraints

import com.amazon.deequ.analyzers._
import com.amazon.deequ.metrics.{Distribution, Metric}
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.util.matching.Regex

object ConstraintStatus extends Enumeration {
  val Success, Failure = Value
}

case class ConstraintResult(
    constraint: Constraint,
    status: ConstraintStatus.Value,
    message: Option[String] = None)

/** Common trait for all data quality constraints */
trait Constraint {
  def evaluate(analysisResults: Map[Analyzer[_, Metric[_]], Metric[_]]): ConstraintResult
}

/** Common trait for constraint decorators */
class ConstraintDecorator(protected val _inner: Constraint) extends Constraint {
  def inner: Constraint = {
    _inner match {
      case dc: ConstraintDecorator => dc.inner
      case _ => _inner
    }
  }

  override def evaluate(
      analysisResults: Map[Analyzer[_, Metric[_]], Metric[_]])
    : ConstraintResult = {

    val result = _inner.evaluate(analysisResults)
    ConstraintResult(this, result.status, result.message)
  }
}

/**
  * Constraint decorator which holds a name of the constraint along with it
  *
  * @param constraint Delegate
  * @param name       Name (Detailed message) for the constraint
  */
class NamedConstraint(private[deequ] val constraint: Constraint, name: String)
    extends ConstraintDecorator(constraint) {
  override def toString(): String = name
}

/**
  * Companion object to create constraint objects
  * These methods can be used from the unit tests or during creation of Check configuration
  */
object Constraint {

  def named(constraint: Constraint, name: String): NamedConstraint = {
    new NamedConstraint(constraint, name)
  }

  /**
    * Runs Size analysis on the given column and executes the assertion
    *
    * @param assertion Function that receives a long input parameter and returns a boolean
    */
  def sizeConstraint(assertion: Long => Boolean, where: Option[String] = None): Constraint = {

    val size = Size(where)

    val constraint = AnalysisBasedConstraint[NumMatches, Double, Long](size,
      assertion, Some(_.toLong))

    new NamedConstraint(constraint, s"SizeConstraint($size)")
  }

  /**
    * Runs Histogram analysis on the given column and executes the assertion
    *
    * @param column     Column to run the assertion on
    * @param assertion  Function that receives a Distribution input parameter.
    *                   E.g "_.ratios("a") <= 0.7)"
    * @param binningUdf Optional binning function to bin the values
    * @param maxBins    Optional maximum bin count to limit the number of metrics created
    */
  def histogramConstraint(
      column: String,
      assertion: Distribution => Boolean,
      binningUdf: Option[UserDefinedFunction] = None,
      maxBins: Integer = Histogram.MaximumAllowedDetailBins)
    : Constraint = {

    val histogram = Histogram(column, binningUdf, maxBins)

    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Distribution, Distribution](
      histogram, assertion)

    new NamedConstraint(constraint, s"HistogramConstraint($histogram)")
  }


  /**
    * Runs Histogram analysis on the given column and executes the assertion
    *
    * @param column     Column to run the assertion on
    * @param assertion  Function that receives a long input parameter and returns a boolean
    * @param binningUdf Optional binning function to bin the values
    * @param maxBins    Optional maximum bin count to limit the number of metrics created
    */
  def histogramBinConstraint(
      column: String,
      assertion: Long => Boolean,
      binningUdf: Option[UserDefinedFunction] = None,
      maxBins: Integer = Histogram.MaximumAllowedDetailBins)
    : Constraint = {

    val histogram = Histogram(column, binningUdf, maxBins)

    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Distribution, Long](
      histogram, assertion,
      Some(_.numberOfBins))

    new NamedConstraint(constraint, s"HistogramBinConstraint($histogram)")
  }

  /**
    * Runs Completeness analysis on the given column and executes the assertion
    *
    * @param column    Column to run the assertion on
    * @param assertion Function that receives a double input parameter (since the metric is
    *                  double metric) and returns a boolean
    */
  def completenessConstraint(
      column: String,
      assertion: Double => Boolean, where: Option[String] = None)
    : Constraint = {

    val completeness = Completeness(column, where)

    val constraint = AnalysisBasedConstraint[NumMatchesAndCount, Double, Double](
      completeness, assertion)

    new NamedConstraint(constraint, s"CompletenessConstraint($completeness)")
  }

  /**
    * Runs Completeness analysis on the given column and executes the anomaly assertion
    *
    * @param analyzer           Analyzer for the metric to do Anomaly Detection on
    * @param anomalyAssertion   Function that receives a double input parameter
    *                           (since the metric is double metric) and returns a boolean
    */
  def anomalyConstraint[S <: State[S]](
      analyzer: Analyzer[S, Metric[Double]],
      anomalyAssertion: Double => Boolean)
    : Constraint = {

    val constraint = AnalysisBasedConstraint[S, Double, Double](analyzer, anomalyAssertion)

    new NamedConstraint(constraint, s"AnomalyConstraint($analyzer)")
  }

  /**
    * Runs Uniqueness analysis on the given columns and executes the assertion
    *
    * @param columns   Columns to run the assertion on
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    */
  def uniquenessConstraint(columns: Seq[String], assertion: Double => Boolean): Constraint = {

    val uniqueness = Uniqueness(columns)

    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      uniqueness, assertion)

    new NamedConstraint(constraint, s"UniquenessConstraint($uniqueness)")
  }

  /**
    * Runs Distinctness analysis on the given columns and executes the assertion
    *
    * @param columns   Columns to run the assertion on
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    */
  def distinctnessConstraint(columns: Seq[String], assertion: Double => Boolean): Constraint = {

    val distinctness = Distinctness(columns)

    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      distinctness, assertion)

    new NamedConstraint(constraint, s"DistinctnessConstraint($distinctness)")
  }

  /**
    * Runs UniqueValueRatio analysis on the given columns and executes the assertion
    *
    * @param columns   Columns to run the assertion on
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    */
  def uniqueValueRatioConstraint(columns: Seq[String], assertion: Double => Boolean): Constraint = {

    val uniqueValueRatio = UniqueValueRatio(columns)
    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      uniqueValueRatio, assertion)

    new NamedConstraint(constraint, s"UniqueValueRatioConstraint($uniqueValueRatio")
  }

  /**
    * Runs given column expression analysis on the given column(s) and executes the assertion
    *
    * @param name A name that summarizes the check being made. This name is being used to name the
    *             metrics for the analysis being done.
    * @param column Data frame column which is a combination of expression and the column name
    */
  def complianceConstraint(
      name: String,
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None)
    : Constraint = {

    val compliance = Compliance(name, column, where)

    val constraint = AnalysisBasedConstraint[NumMatchesAndCount, Double, Double](
      compliance, assertion)

    new NamedConstraint(constraint, s"ComplianceConstraint($compliance)")
  }

  def patternMatchConstraint(
      column: String,
      pattern: Regex,
      assertion: Double => Boolean,
      where: Option[String] = None,
      name: Option[String] = None)
    : Constraint = {

    val patternMatch = PatternMatch(column, pattern, where)

    val constraint = AnalysisBasedConstraint[NumMatchesAndCount, Double, Double](
      patternMatch, assertion)

    val constraintName = name match {
      case Some(aName) => aName
      case _ => s"PatternMatchConstraint($column, $pattern)"
    }

    new NamedConstraint(constraint, constraintName)
  }

  /**
    * Runs Entropy analysis on the given column and executes the assertion
    *
    * @param column    Column to run the assertion on
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    */
  def entropyConstraint(column: String, assertion: Double => Boolean): Constraint = {

    val entropy = Entropy(column)

    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      entropy, assertion)

    new NamedConstraint(constraint, s"EntropyConstraint($entropy)")
  }

  /**
    * Runs mutual information analysis on the given columns and executes the assertion
    *
    * @param columnA   First of the column pair that the mutual information will be calculated upon
    * @param columnB   The other column that mutual information will be calculated upon
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    */
  def mutualInformationConstraint(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean)
    : Constraint = {

    val mutualInformation = MutualInformation(Seq(columnA, columnB))

    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      mutualInformation, assertion)

    new NamedConstraint(constraint, s"MutualInformationConstraint($mutualInformation)")
  }

  /**
    * Runs approximate quantile analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param quantile Which quantile to assert on
    * @param assertion Function that receives a double input parameter (the computed quantile)
    *                  and returns a boolean
    */
  def approxQuantileConstraint(
      column: String,
      quantile: Double,
      assertion: Double => Boolean)
    : Constraint = {

    val approxQuantile = ApproxQuantile(column, quantile)

    val constraint = AnalysisBasedConstraint[ApproxQuantileState, Double, Double](
      approxQuantile, assertion)

    new NamedConstraint(constraint, s"ApproxQuantileConstraint($approxQuantile)")
  }

  /**
    * Runs minimum analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    */
  def minConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None): Constraint = {

    val minimum = Minimum(column, where)

    val constraint = AnalysisBasedConstraint[MinState, Double, Double](minimum, assertion)

    new NamedConstraint(constraint, s"MinimumConstraint($minimum)")
  }

  /**
    * Runs maximum analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    */
  def maxConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None): Constraint = {

    val maximum = Maximum(column, where)

    val constraint = AnalysisBasedConstraint[MaxState, Double, Double](maximum, assertion)

    new NamedConstraint(constraint, s"MaximumConstraint($maximum)")
  }

  /**
    * Runs mean analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    */
  def meanConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None): Constraint = {

    val mean = Mean(column, where)

    val constraint = AnalysisBasedConstraint[MeanState, Double, Double](mean, assertion)

    new NamedConstraint(constraint, s"MeanConstraint($mean)")
  }
  /**
    * Runs sum analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    */
  def sumConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None): Constraint = {

    val sum = Sum(column, where)

    val constraint = AnalysisBasedConstraint[SumState, Double, Double](sum, assertion)

    new NamedConstraint(constraint, s"SumConstraint($sum)")
  }


  /**
    * Runs standard deviation analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    */
  def standardDeviationConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None)
    : Constraint = {

    val standardDeviation = StandardDeviation(column, where)

    val constraint = AnalysisBasedConstraint[StandardDeviationState, Double, Double](
      standardDeviation, assertion)

    new NamedConstraint(constraint, s"StandardDeviationConstraint($standardDeviation)")
  }

  /**
    * Runs approximate count distinct analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    */
  def approxCountDistinctConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None)
    : Constraint = {

    val approxCountDistinct = ApproxCountDistinct(column, where)

    val constraint = AnalysisBasedConstraint[ApproxCountDistinctState, Double, Double](
      approxCountDistinct, assertion)

    new NamedConstraint(constraint, s"ApproxCountDistinctConstraint($approxCountDistinct)")
  }

  /**
    * Runs pearson correlation analysis on the given columns and executes the assertion
    *
    * @param columnA   First column for pearson correlation
    * @param columnB   Second column for pearson correlation
    * @param assertion Function that receives a double input parameter and returns a boolean
    */
  def correlationConstraint(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean,
      where: Option[String] = None)
    : Constraint = {

    val correlation = Correlation(columnA, columnB, where)

    val constraint = AnalysisBasedConstraint[CorrelationState, Double, Double](
      correlation, assertion)

    new NamedConstraint(constraint, s"CorrelationConstraint($correlation)")
  }

  /**
    * Runs data type analyzer on the given column and executes the assertion on the data type ratio.
    *
    * @param column Column to compute the data type distribution for.
    * @param dataType The data type that should be checked in the assertion.
    * @param assertion Function from the ratio of the data type in the specified column to boolean.
    * @return
    */
  def dataTypeConstraint(
      column: String,
      dataType: ConstrainableDataTypes.Value,
      assertion: Double => Boolean)
    : Constraint = {

    /** Get the specified data type distribution ratio or maps to 0.0 */
    def valuePicker(distribution: Distribution): Double = {

      dataType match {
        case ConstrainableDataTypes.Null => ratio(DataTypeInstances.Unknown, distribution)
        case ConstrainableDataTypes.Fractional => ratio(DataTypeInstances.Fractional, distribution)
        case ConstrainableDataTypes.Integral => ratio(DataTypeInstances.Integral, distribution)
        case ConstrainableDataTypes.Boolean => ratio(DataTypeInstances.Boolean, distribution)
        case ConstrainableDataTypes.String => ratio(DataTypeInstances.String, distribution)
        case ConstrainableDataTypes.Numeric =>
          ratio(DataTypeInstances.Fractional, distribution) +
            ratio(DataTypeInstances.Integral, distribution)
      }
    }

    AnalysisBasedConstraint[DataTypeHistogram, Distribution, Double](DataType(column), assertion,
      Some(valuePicker))
  }

  private[this] def ratio(keyType: DataTypeInstances.Value, distribution: Distribution): Double = {
    distribution.values.get(keyType.toString)
      .map { _.ratio }
      .getOrElse(0.0)
  }

}
