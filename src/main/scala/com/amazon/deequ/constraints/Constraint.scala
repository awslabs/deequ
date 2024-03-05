/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.deequ.metrics.BucketDistribution
import com.amazon.deequ.metrics.Distribution
import com.amazon.deequ.metrics.Metric
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.util.Failure
import scala.util.Success
import scala.util.matching.Regex

object ConstraintStatus extends Enumeration {
  val Success, Failure = Value
}

case class ConstraintResult(
    constraint: Constraint,
    status: ConstraintStatus.Value,
    message: Option[String] = None,
    metric: Option[Metric[_]] = None)

/** Common trait for all data quality constraints */
trait Constraint extends Serializable {
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

    // most of the constraints are of type NamedConstraint
    // having `this` as the constraint of the result to
    // keep the more informative .toString of NamedConstraint
    _inner.evaluate(analysisResults).copy(constraint = this)
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
 * Constraint decorator which holds a name of the constraint and a name for the column-level result
 *
 * @param constraint Delegate
 * @param name       Name (Detailed message) for the constraint
 * @param columnName Name for the column containing row-level results for this constraint
 */
class RowLevelConstraint(private[deequ] override val constraint: Constraint, name: String, columnName: String)
  extends NamedConstraint(constraint, name) {
  val getColumnName: String = columnName
}

class RowLevelAssertedConstraint(private[deequ] override val constraint: Constraint,
                                 name: String,
                                 columnName: String,
                                 val assertion: UserDefinedFunction)
  extends RowLevelConstraint(constraint, name, columnName) {
}

class RowLevelGroupedConstraint(private[deequ] override val constraint: Constraint,
                                name: String,
                                columns: Seq[String])
  extends NamedConstraint(constraint, name) {
  val getColumnName: String = columns.head
}

/**
  * Companion object to create constraint objects
  * These methods can be used from the unit tests or during creation of Check configuration
  */
object Constraint {

  /**
    * Runs Size analysis on the given column and executes the assertion
    *
    * @param assertion Function that receives a long input parameter and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    */
  def sizeConstraint(
      assertion: Long => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val size = Size(where)

    fromAnalyzer(size, assertion, hint)
  }

  def fromAnalyzer(size: Size, assertion: Long => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[NumMatches, Double, Long](size,
      assertion, Some(_.toLong), hint)

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
    * @param where Additional filter to apply before the analyzer is run.
    * @param hint A hint to provide additional context why a constraint could have failed
    */
  def histogramConstraint(
      column: String,
      assertion: Distribution => Boolean,
      binningUdf: Option[UserDefinedFunction] = None,
      maxBins: Integer = Histogram.MaximumAllowedDetailBins,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val histogram = Histogram(column, binningUdf, maxBins, where)

    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Distribution, Distribution](
      histogram, assertion, hint = hint)

    new NamedConstraint(constraint, s"HistogramConstraint($histogram)")
  }


  /**
    * Runs Histogram analysis on the given column and executes the assertion
    *
    * @param column     Column to run the assertion on
    * @param assertion  Function that receives a long input parameter and returns a boolean
    * @param binningUdf Optional binning function to bin the values
    * @param maxBins    Optional maximum bin count to limit the number of metrics created
    * @param where Additional filter to apply before the analyzer is run.
    * @param hint A hint to provide additional context why a constraint could have failed
    */
  def histogramBinConstraint(
      column: String,
      assertion: Long => Boolean,
      binningUdf: Option[UserDefinedFunction] = None,
      maxBins: Integer = Histogram.MaximumAllowedDetailBins,
      where: Option[String] = None,
      hint: Option[String] = None,
      computeFrequenciesAsRatio: Boolean = true)
    : Constraint = {

    val histogram = Histogram(column, binningUdf, maxBins, where, computeFrequenciesAsRatio)

    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Distribution, Long](
      histogram, assertion, Some(_.numberOfBins), hint)

    new NamedConstraint(constraint, s"HistogramBinConstraint($histogram)")
  }

  /**
    * Runs Completeness analysis on the given column and executes the assertion
    *
    * @param column    Column to run the assertion on
    * @param assertion Function that receives a double input parameter (since the metric is
    *                  double metric) and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    * @param analyzerOptions Options to configure analyzer behavior (NullTreatment, FilteredRow)
    */
  def completenessConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None,
      analyzerOptions: Option[AnalyzerOptions] = None)
    : Constraint = {

    val completeness = Completeness(column, where, analyzerOptions)

    this.fromAnalyzer(completeness, assertion, hint)
  }

  def fromAnalyzer(completeness: Completeness,
                   assertion: Double => Boolean,
                   hint: Option[String] = None): Constraint = {
    val constraint = AnalysisBasedConstraint[NumMatchesAndCount, Double, Double](
      completeness, assertion, hint = hint)

    new RowLevelConstraint(constraint, s"CompletenessConstraint($completeness)", s"Completeness-${completeness.column}")
  }

  /**
    * Runs Completeness analysis on the given column and executes the anomaly assertion
    *
    * @param analyzer           Analyzer for the metric to do Anomaly Detection on
    * @param anomalyAssertion   Function that receives a double input parameter
    *                           (since the metric is double metric) and returns a boolean
    * @param hint A hint to provide additional context why a constraint could have failed
    */
  def anomalyConstraint[S <: State[S]](
      analyzer: Analyzer[S, Metric[Double]],
      anomalyAssertion: Double => Boolean,
      hint: Option[String] = None)
    : Constraint = {

    val constraint = AnalysisBasedConstraint[S, Double, Double](analyzer, anomalyAssertion,
      hint = hint)

    new NamedConstraint(constraint, s"AnomalyConstraint($analyzer)")
  }

  /**
    * Runs Uniqueness analysis on the given columns and executes the assertion
    *
    * @param columns   Columns to run the assertion on
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    * @param where Additional filter to apply before the analyzer is run.
    * @param hint A hint to provide additional context why a constraint could have failed
    * @param analyzerOptions Options to configure analyzer behavior (NullTreatment, FilteredRow)
    */
  def uniquenessConstraint(
      columns: Seq[String],
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None,
      analyzerOptions: Option[AnalyzerOptions] = None)
    : Constraint = {

    val uniqueness = Uniqueness(columns, where, analyzerOptions)

    fromAnalyzer(uniqueness, assertion, hint)
  }

  def fromAnalyzer(uniqueness: Uniqueness, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      uniqueness, assertion, hint = hint)

    new RowLevelGroupedConstraint(constraint,
      s"UniquenessConstraint($uniqueness)",
      uniqueness.columns)
  }

  /**
    * Runs Distinctness analysis on the given columns and executes the assertion
    *
    * @param columns   Columns to run the assertion on
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    * @param where Additional filter to apply before the analyzer is run.
    * @param hint A hint to provide additional context why a constraint could have failed
    */
  def distinctnessConstraint(
      columns: Seq[String],
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val distinctness = Distinctness(columns, where)

    fromAnalyzer(distinctness, assertion, hint)
  }

  def fromAnalyzer(distinctness: Distinctness, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      distinctness, assertion, hint = hint)

    new NamedConstraint(constraint, s"DistinctnessConstraint($distinctness)")
  }

  /**
    * Runs UniqueValueRatio analysis on the given columns and executes the assertion
    *
    * @param columns   Columns to run the assertion on
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    * @param where Additional filter to apply before the analyzer is run.
    * @param hint A hint to provide additional context why a constraint could have failed
    * @param analyzerOptions Options to configure analyzer behavior (NullTreatment, FilteredRow)
    */
  def uniqueValueRatioConstraint(
      columns: Seq[String],
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None,
      analyzerOptions: Option[AnalyzerOptions] = None)
    : Constraint = {

    val uniqueValueRatio = UniqueValueRatio(columns, where, analyzerOptions)
    fromAnalyzer(uniqueValueRatio, assertion, hint)
  }

  def fromAnalyzer(
      uniqueValueRatio: UniqueValueRatio,
      assertion: Double => Boolean,
      hint: Option[String])
    : Constraint = {
    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      uniqueValueRatio, assertion, hint = hint)

    new RowLevelGroupedConstraint(constraint,
      s"UniqueValueRatioConstraint($uniqueValueRatio",
      uniqueValueRatio.columns)
  }

  /**
    * Runs given column expression analysis on the given column(s) and executes the assertion
    *
    * @param name A name that summarizes the check being made. This name is being used to name the
    *             metrics for the analysis being done.
    * @param column Data frame column which is a combination of expression and the column name
    * @param hint A hint to provide additional context why a constraint could have failed
    * @param analyzerOptions Options to configure analyzer behavior (NullTreatment, FilteredRow)
    */
  def complianceConstraint(
      name: String,
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None,
      columns: List[String] = List.empty[String],
      analyzerOptions: Option[AnalyzerOptions] = None)
    : Constraint = {

    val compliance = Compliance(name, column, where, columns, analyzerOptions)

    fromAnalyzer(compliance, assertion, hint)
  }

  private def fromAnalyzer(compliance: Compliance, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[NumMatchesAndCount, Double, Double](
      compliance, assertion, hint = hint)

    val sparkAssertion = org.apache.spark.sql.functions.udf(assertion)
    new RowLevelAssertedConstraint(
      constraint,
      s"ComplianceConstraint($compliance)",
      s"ColumnsCompliance-${compliance.predicate}",
      sparkAssertion)
  }

  /**
    * Runs given regex compliance analysis on the given column(s) and executes the assertion
    *
    * @param name    A name that summarizes the check being made. This name is being used
    *                to name the metrics for the analysis being done.
    * @param pattern The regex pattern to check compliance for
    * @param column  Data frame column which is a combination of expression and the column name
    * @param hint    A hint to provide additional context why a constraint could have failed
    * @param analyzerOptions Options to configure analyzer behavior (NullTreatment, FilteredRow)
    */
  def patternMatchConstraint(
      column: String,
      pattern: Regex,
      assertion: Double => Boolean,
      where: Option[String] = None,
      name: Option[String] = None,
      hint: Option[String] = None,
      analyzerOptions: Option[AnalyzerOptions] = None)
    : Constraint = {

    val patternMatch = PatternMatch(column, pattern, where, analyzerOptions)

    fromAnalyzer(patternMatch, pattern, assertion, name, hint)
  }

  def fromAnalyzer(
                    patternMatch: PatternMatch,
                    pattern: Regex,
                    assertion: Double => Boolean,
                    name: Option[String],
                    hint: Option[String]): Constraint = {
    val column: String = patternMatch.column
    val constraint = AnalysisBasedConstraint[NumMatchesAndCount, Double, Double](
      patternMatch, assertion, hint = hint)

    val constraintName = name match {
      case Some(aName) => aName
      case _ => s"PatternMatchConstraint($column, $pattern)"
    }

    new RowLevelConstraint(constraint, constraintName, s"ColumnPattern-$column")
  }

  /**
    * Runs Entropy analysis on the given column and executes the assertion
    *
    * @param column    Column to run the assertion on
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    * @param where Additional filter to apply before the analyzer is run.
    * @param hint    A hint to provide additional context why a constraint could have failed
    */
  def entropyConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val entropy = Entropy(column, where)

    fromAnalyzer(entropy, assertion, hint)
  }

  def fromAnalyzer(entropy: Entropy, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      entropy, assertion, hint = hint)

    new NamedConstraint(constraint, s"EntropyConstraint($entropy)")
  }

  /**
    * Runs mutual information analysis on the given columns and executes the assertion
    *
    * @param columnA   First of the column pair that the mutual information will be calculated upon
    * @param columnB   The other column that mutual information will be calculated upon
    * @param assertion Function that receives a double input parameter
    *                  (since the metric is double metric) and returns a boolean
    * @param where Additional filter to apply before the analyzer is run.
    * @param hint    A hint to provide additional context why a constraint could have failed
    */
  def mutualInformationConstraint(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val mutualInformation = MutualInformation(Seq(columnA, columnB), where)

    fromAnalyzer(mutualInformation, assertion, hint)
  }

  def fromAnalyzer(
      mutualInformation: MutualInformation,
      assertion: Double => Boolean,
      hint: Option[String])
    : Constraint = {
    val constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, Double, Double](
      mutualInformation, assertion, hint = hint)

    new NamedConstraint(constraint, s"MutualInformationConstraint($mutualInformation)")
  }

  /**
    * Runs approximate quantile analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param quantile Which quantile to assert on
    * @param assertion Function that receives a double input parameter (the computed quantile)
    *                  and returns a boolean
    * @param where Additional filter to apply before the analyzer is run.
    * @param hint    A hint to provide additional context why a constraint could have failed
    */
  def approxQuantileConstraint(
      column: String,
      quantile: Double,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val approxQuantile = ApproxQuantile(column, quantile, where = where)

    fromAnalyzer(approxQuantile, assertion, hint)
  }

  def fromAnalyzer(approxQuantile: ApproxQuantile, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[ApproxQuantileState, Double, Double](
      approxQuantile, assertion, hint = hint)

    new NamedConstraint(constraint, s"ApproxQuantileConstraint($approxQuantile)")
  }

  /**
   * Runs exact quantile analysis on the given column and executes the assertion
   *
   * @param column    Column to run the assertion on
   * @param quantile  Which quantile to assert on
   * @param assertion Function that receives a double input parameter (the computed quantile)
   *                  and returns a boolean
   * @param where     Additional filter to apply before the analyzer is run.
   * @param hint      A hint to provide additional context why a constraint could have failed
   */
  def exactQuantileConstraint(
                                column: String,
                                quantile: Double,
                                assertion: Double => Boolean,
                                where: Option[String] = None,
                                hint: Option[String] = None)
  : Constraint = {

    val exactQuantile = ExactQuantile(column, quantile, where = where)

    fromAnalyzer(exactQuantile, assertion, hint)
  }

  def fromAnalyzer(exactQuantile: ExactQuantile, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[ExactQuantileState, Double, Double](
      exactQuantile, assertion, hint = hint)

    new NamedConstraint(constraint, s"ExactQuantileConstraint($exactQuantile)")
  }

  /**
    * Runs max length analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint    A hint to provide additional context why a constraint could have failed
    * @param analyzerOptions Options to configure analyzer behavior (NullTreatment, FilteredRow)
    */
  def maxLengthConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None,
      analyzerOptions: Option[AnalyzerOptions] = None)
    : Constraint = {

    val maxLength = MaxLength(column, where, analyzerOptions)

    fromAnalyzer(maxLength, assertion, hint)
  }

  def fromAnalyzer(maxLength: MaxLength, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val column: String = maxLength.column
    val constraint = AnalysisBasedConstraint[MaxState, Double, Double](maxLength, assertion,
      hint = hint)

    val sparkAssertion = org.apache.spark.sql.functions.udf(assertion)

    new RowLevelAssertedConstraint(
      constraint,
      s"MaxLengthConstraint($maxLength)",
      s"ColumnLength-$column",
      sparkAssertion)
  }

  /**
    * Runs min length analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint    A hint to provide additional context why a constraint could have failed
    * @param analyzerOptions Options to configure analyzer behavior (NullTreatment, FilteredRow)
    */
  def minLengthConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None,
      analyzerOptions: Option[AnalyzerOptions] = None)
    : Constraint = {

    val minLength = MinLength(column, where, analyzerOptions)

    fromAnalyzer(minLength, assertion, hint)
  }

  def fromAnalyzer(minLength: MinLength, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val column: String = minLength.column
    val constraint = AnalysisBasedConstraint[MinState, Double, Double](minLength, assertion,
      hint = hint)

    val sparkAssertion = org.apache.spark.sql.functions.udf(assertion)

    new RowLevelAssertedConstraint(
      constraint,
      s"MinLengthConstraint($minLength)",
      s"ColumnLength-$column",
      sparkAssertion)
  }

  /**
    * Runs minimum analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint    A hint to provide additional context why a constraint could have failed
    * @param analyzerOptions Options to configure analyzer behavior (NullTreatment, FilteredRow)
    *
    */
  def minConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None,
      analyzerOptions: Option[AnalyzerOptions] = None)
    : Constraint = {

    val minimum = Minimum(column, where, analyzerOptions)

    fromAnalyzer(minimum, assertion, hint)
  }

  def fromAnalyzer(minimum: Minimum, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val column: String = minimum.column
    val constraint = AnalysisBasedConstraint[MinState, Double, Double](minimum, assertion,
      hint = hint)

    val updatedAssertion = getUpdatedRowLevelAssertion(assertion, minimum.analyzerOptions)
    val sparkAssertion = org.apache.spark.sql.functions.udf(updatedAssertion)

    new RowLevelAssertedConstraint(
      constraint,
      s"MinimumConstraint($minimum)",
      s"ColumnMax-$column",
      sparkAssertion)
  }

  /**
    * Runs maximum analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint    A hint to provide additional context why a constraint could have failed
    * @param analyzerOptions Options to configure analyzer behavior (NullTreatment, FilteredRow)
    */
  def maxConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None,
      analyzerOptions: Option[AnalyzerOptions] = None)
    : Constraint = {

    val maximum = Maximum(column, where, analyzerOptions)

    fromAnalyzer(maximum, assertion, hint)
  }

  def fromAnalyzer(maximum: Maximum, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val column: String = maximum.column
    val constraint = AnalysisBasedConstraint[MaxState, Double, Double](maximum, assertion,
      hint = hint)

    val updatedAssertion = getUpdatedRowLevelAssertion(assertion, maximum.analyzerOptions)
    val sparkAssertion = org.apache.spark.sql.functions.udf(updatedAssertion)

    new RowLevelAssertedConstraint(
      constraint,
      s"MaximumConstraint($maximum)",
      s"ColumnMax-$column",
      sparkAssertion)
  }

  /**
    * Runs mean analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint    A hint to provide additional context why a constraint could have failed
    */
  def meanConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val mean = Mean(column, where)

    fromAnalyzer(mean, assertion, hint)
  }

  def fromAnalyzer(mean: Mean, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[MeanState, Double, Double](mean, assertion,
      hint = hint)

    new NamedConstraint(constraint, s"MeanConstraint($mean)")
  }

  /**
    * Runs sum analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint    A hint to provide additional context why a constraint could have failed
    */
  def sumConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val sum = Sum(column, where)

    fromAnalyzer(sum, assertion, hint)
  }

  def fromAnalyzer(sum: Sum, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[SumState, Double, Double](sum, assertion,
      hint = hint)

    new NamedConstraint(constraint, s"SumConstraint($sum)")
  }

  /**
    * Runs standard deviation analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint    A hint to provide additional context why a constraint could have failed
    */
  def standardDeviationConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val standardDeviation = StandardDeviation(column, where)

    fromAnalyzer(standardDeviation, assertion, hint)
  }

  def fromAnalyzer(
      standardDeviation: StandardDeviation,
      assertion: Double => Boolean,
      hint: Option[String])
    : Constraint = {
    val constraint = AnalysisBasedConstraint[StandardDeviationState, Double, Double](
      standardDeviation, assertion, hint = hint)

    new NamedConstraint(constraint, s"StandardDeviationConstraint($standardDeviation)")
  }

  /**
    * Runs approximate count distinct analysis on the given column and executes the assertion
    *
    * @param column Column to run the assertion on
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint    A hint to provide additional context why a constraint could have failed
    */
  def approxCountDistinctConstraint(
      column: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val approxCountDistinct = ApproxCountDistinct(column, where)

    fromAnalyzer(approxCountDistinct, assertion, hint)
  }

  def fromAnalyzer(
      approxCountDistinct: ApproxCountDistinct,
      assertion: Double => Boolean,
      hint: Option[String])
    : Constraint = {
    val constraint = AnalysisBasedConstraint[ApproxCountDistinctState, Double, Double](
      approxCountDistinct, assertion, hint = hint)

    new NamedConstraint(constraint, s"ApproxCountDistinctConstraint($approxCountDistinct)")
  }

  /**
    * Runs pearson correlation analysis on the given columns and executes the assertion
    *
    * @param columnA   First column for pearson correlation
    * @param columnB   Second column for pearson correlation
    * @param assertion Function that receives a double input parameter and returns a boolean
    * @param hint      A hint to provide additional context why a constraint could have failed
    */
  def correlationConstraint(
      columnA: String,
      columnB: String,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    val correlation = Correlation(columnA, columnB, where)

    fromAnalyzer(correlation, assertion, hint)
  }

  def fromAnalyzer(correlation: Correlation, assertion: Double => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[CorrelationState, Double, Double](
      correlation, assertion, hint = hint)

    new NamedConstraint(constraint, s"CorrelationConstraint($correlation)")
  }

  /**
    * Runs data type analyzer on the given column and executes the assertion on the data type ratio.
    *
    * @param column Column to compute the data type distribution for.
    * @param dataType The data type that should be checked in the assertion.
    * @param assertion Function from the ratio of the data type in the specified column to boolean.
    * @param where Additional filter to apply before the analyzer is run.
    * @param hint A hint to provide additional context why a constraint could have failed
    * @return
    */
  def dataTypeConstraint(
      column: String,
      dataType: ConstrainableDataTypes.Value,
      assertion: Double => Boolean,
      where: Option[String] = None,
      hint: Option[String] = None)
    : Constraint = {

    /** Get the specified data type distribution ratio or maps to 0.0 */
    val valuePicker: Distribution => Double = {
      val pure = ratioTypes(ignoreUnknown = true) _
      import DataTypeInstances._
      dataType match {
        case ConstrainableDataTypes.Null => ratioTypes(ignoreUnknown = false)(Unknown)
        case ConstrainableDataTypes.Fractional => pure(Fractional)
        case ConstrainableDataTypes.Integral => pure(Integral)
        case ConstrainableDataTypes.Boolean => pure(Boolean)
        case ConstrainableDataTypes.String => pure(String)
        case ConstrainableDataTypes.Numeric => d => pure(Fractional)(d) + pure(Integral)(d)
      }
    }

    AnalysisBasedConstraint[DataTypeHistogram, Distribution, Double](DataType(column, where),
      assertion, Some(valuePicker), hint)
  }

  /**
   * Runs kll analyzer on the given column and executes the assertion on the BucketDistribution.
   *
   * @param column Column to compute the BucketDistribution for.
   * @param assertion  Function from BucketDistribution in the specified column to boolean.
   * @param kllParameters parameters of KLL Sketch
   * @param hint A hint to provide additional context why a constraint could have failed
   */
  def kllConstraint(
                     column: String,
                     assertion: BucketDistribution => Boolean,
                     kllParameters: Option[KLLParameters] = None,
                     hint: Option[String] = None)
    : Constraint = {

    val kllSketch = KLLSketch(column, kllParameters = kllParameters)

    fromAnalyzer(kllSketch, assertion, hint)
  }

  def fromAnalyzer(kllSketch: KLLSketch, assertion: BucketDistribution => Boolean, hint: Option[String]): Constraint = {
    val constraint = AnalysisBasedConstraint[KLLState, BucketDistribution, BucketDistribution](
      kllSketch, assertion, hint = hint)

    new NamedConstraint(constraint, s"kllSketchConstraint($kllSketch)")
  }

  /**
    * Calculates the ratio of the key type against the rest of the distribution's values.
    *
    * If `ignoreUnknown` is `true`, then all null or otherwise unknown counting values are
    * disregarded in the data type ratio calculation. Otherwise these Unknown values are
    * considered in the data type ratio calculation.
    *
    * This function evaluates to 0.0 iff there were 0 values of the given key type in the
    * distribution or if there are no other values in the distribution (either before or after
    * discounting `Unknown` typed values).
    *
    * @param ignoreUnknown If `true`, then all Unknown values are ignored. O/w they are included.
    * @param keyType The column data type that we are analyzing.
    * @param distribution The distribution of values, by data type, in the column.
    * @return Ratio of key-typed values to rest of (potentially non-null) values.
    */
  private[this] def ratioTypes(ignoreUnknown: Boolean)(keyType: DataTypeInstances.Value)(
      distribution: Distribution)
    : Double =
    if (ignoreUnknown) {
      val absoluteCount = distribution.values
        .get(keyType.toString)
        .map { _.absolute }
        .getOrElse(0L)
      if (absoluteCount == 0L) {
        0.0
      } else {
        val numValues = distribution.values.values.map { _.absolute }.sum
        val numUnknown = distribution.values
          .get(DataTypeInstances.Unknown.toString())
          .map { _.absolute }
          .getOrElse(0L)
        val sumOfNonNull = numValues - numUnknown
        absoluteCount.toDouble / sumOfNonNull.toDouble
      }
    } else {
      distribution.values
        .get(keyType.toString)
        .map { _.ratio }
        .getOrElse(0.0)
    }

  private[this] def getUpdatedRowLevelAssertion(assertion: Double => Boolean,
                                                analyzerOpts: Option[AnalyzerOptions])
  : java.lang.Double => java.lang.Boolean = {
    (d: java.lang.Double) => {
      if (Option(d).isDefined) assertion(d)
      else analyzerOpts match {
        case Some(analyzerOptions) => analyzerOptions.filteredRow match {
          case FilteredRowOutcome.TRUE => true
          case FilteredRowOutcome.NULL => null
        }
        case None => null
      }
    }
  }
}

/**
 * DatasetMatch Constraint
 * @param analyzer Data Synchronization Analyzer
 * @param hint hint
 */
case class DatasetMatchConstraint(analyzer: DatasetMatchAnalyzer, hint: Option[String])
  extends Constraint {

  override def evaluate(metrics: Map[Analyzer[_, Metric[_]], Metric[_]]): ConstraintResult = {

    metrics.collectFirst {
      case (_: DatasetMatchAnalyzer, metric: Metric[Double]) => metric
    } match {
      case Some(metric) =>
        val result = metric.value match {
          case Success(value) => analyzer.assertion(value)
          case Failure(_) => false
        }
        val status = if (result) ConstraintStatus.Success else ConstraintStatus.Failure
        ConstraintResult(this, status, hint, Some(metric))

      case None =>
        ConstraintResult(this, ConstraintStatus.Failure, hint, None)
    }
  }
}
