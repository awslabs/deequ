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

import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.anomalydetection.{AnomalyAssertionResult, AnomalyThreshold, DetectionResult}
import com.amazon.deequ.metrics.Metric
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
 * Case class for anomaly based constraints that provides unified way to access
 * AnalyzerContext and metrics stored in it.
 * TODO this differs from AnalysisBasedConstraint only in that it uses an assertion function that
 * TODO returns an AnomalyAssertionResult with a potential anomaly as well as the default boolean
 * TODO figure out if it's better to use some inheritance/composition
 *
 * Runs the analysis and get the value of the metric returned by the analysis,
 * picks the numeric value that will be used in the assertion function with metric picker
 * runs the assertion.
 *
 * @param analyzer    Analyzer to be run on the data frame
 * @param assertion   Assertion function that returns an AnomalyAssertionResult with a potential anomaly
 * @param valuePicker Optional function to pick the interested part of the metric value that the
 *                    assertion will be running on. Absence of such function means the metric
 *                    value would be used in the assertion as it is.
 * @param hint A hint to provide additional context why a constraint could have failed
 * @tparam M : Type of the metric value generated by the Analyzer
 * @tparam V : Type of the value being used in assertion function
 *
 */
private[deequ] case class AnomalyBasedConstraint[S <: State[S], M, V](
                                                     analyzer: Analyzer[S, Metric[M]],
                                                     private[deequ] val assertion: V => AnomalyAssertionResult,
                                                     private[deequ] val valuePicker: Option[M => V] = None,
                                                     private[deequ] val hint: Option[String] = None)
  extends Constraint {

  private[deequ] def calculateAndEvaluate(data: DataFrame) = {
    val metric = analyzer.calculate(data)
    evaluate(Map(analyzer -> metric))
  }

  override def evaluate(
                         analysisResults: Map[Analyzer[_, Metric[_]], Metric[_]])
  : ConstraintResult = {

    val metric = analysisResults.get(analyzer).map(_.asInstanceOf[Metric[M]])

    metric.map(pickValueAndAssert).getOrElse(
      // Analysis is missing
      ConstraintResult(this, ConstraintStatus.Failure,
        message = Some(AnomalyBasedConstraint.MissingAnalysis), metric = metric)
    )
  }

  private[this] def pickValueAndAssert(metric: Metric[M]): ConstraintResult = {

    metric.value match {
      // Analysis done successfully and result metric is there
      case Success(metricValue) =>
        try {
          val assertOn = runPickerOnMetric(metricValue)
          val anomalyAssertionResult = runAssertion(assertOn)

          if (anomalyAssertionResult.hasNoAnomaly) {
            ConstraintResult(this, ConstraintStatus.Success, metric = Some(metric),
              anomaly = anomalyAssertionResult.anomaly)
          } else {
            var errorMessage = s"Value: $assertOn does not meet the constraint requirement, has anomaly, check result!"
            hint.foreach(hint => errorMessage += s" $hint")

            ConstraintResult(this, ConstraintStatus.Failure, Some(errorMessage), Some(metric),
              anomaly = anomalyAssertionResult.anomaly)
          }

        } catch {
          case AnomalyBasedConstraint.ConstraintAssertionException(msg) =>
            ConstraintResult(this, ConstraintStatus.Failure,
              message = Some(s"${AnomalyBasedConstraint.AssertionException}: $msg!"), metric = Some(metric))
          case AnomalyBasedConstraint.ValuePickerException(msg) =>
            ConstraintResult(this, ConstraintStatus.Failure,
              message = Some(s"${AnomalyBasedConstraint.ProblematicMetricPicker}: $msg!"), metric = Some(metric))
        }
      // An exception occurred during analysis
      case Failure(e) => ConstraintResult(this,
        ConstraintStatus.Failure, message = Some(e.getMessage), metric = Some(metric))
    }
  }

  private def runPickerOnMetric(metricValue: M): V =
    try {
      valuePicker.map(function => function(metricValue)).getOrElse(metricValue.asInstanceOf[V])
    } catch {
      case e: Exception => throw AnomalyBasedConstraint.ValuePickerException(e.getMessage)
    }

  private def runAssertion(assertOn: V): AnomalyAssertionResult =
    try {
      assertion(assertOn)
    } catch {
      case e: Exception => throw AnomalyBasedConstraint.ConstraintAssertionException(e.getMessage)
    }

  // 'assertion' and 'valuePicker' are lambdas we have to represent them like '<function1>'
  override def toString: String =
    s"AnomalyBasedConstraint($analyzer,<function1>,${valuePicker.map(_ => "<function1>")},$hint)"
}

private[deequ] object AnomalyBasedConstraint {
  val MissingAnalysis = "Missing Analysis, can't run the constraint!"
  val ProblematicMetricPicker = "Can't retrieve the value to assert on"
  val AssertionException = "Can't execute the assertion"

  private case class ValuePickerException(message: String) extends RuntimeException(message)
  private case class ConstraintAssertionException(message: String) extends RuntimeException(message)
}
