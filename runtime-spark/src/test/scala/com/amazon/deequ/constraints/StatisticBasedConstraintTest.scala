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

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers._
import com.amazon.deequ.constraints.ConstraintUtils.calculate
import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}
import com.amazon.deequ.runtime.spark.SparkEngine
import com.amazon.deequ.statistics.{Completeness, Statistic}
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.DataFrame
import com.amazon.deequ.TestUtils.calculateSingle
import com.amazon.deequ.runtime.spark.executor.MetricCalculationException
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, PrivateMethodTester, WordSpec}

class StatisticBasedConstraintTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport with MockFactory with PrivateMethodTester {

  /**
    * Sample function to use as value picker
    *
    * @return Returns input multiplied by 2
    */
  def valueDoubler(value: Double): Double = {
    value * 2
  }


  "Analysis based constraint" should {

    "assert correctly on values if analysis is successful" in
      withSparkSession { sparkSession =>
        val df = getDfMissing(sparkSession)

        // Analysis result should equal to 1.0 for an existing column
        val resultA = calculate(StatisticConstraint[Double, Double](Completeness("item"), _ == 1.0), df)

        assert(resultA.status == ConstraintStatus.Success)
        assert(resultA.message.isEmpty)
        assert(resultA.metric.isDefined)

        // Analysis result should equal to 1.0 for an existing column
        val resultB = calculate(StatisticConstraint[Double, Double](Completeness("item"), _ != 1.0), df)

        assert(resultB.status == ConstraintStatus.Failure)
        assert(resultB.message.contains("Value: 1.0 does not meet the constraint requirement!"))
        assert(resultB.metric.isDefined)

        // Analysis should fail for a non existing column
        val resultC = calculate(StatisticConstraint[Double, Double](
          Completeness("someMissingColumn"), _ == 1.0), df)

        assert(resultC.status == ConstraintStatus.Failure)
        assert(resultC.message.contains(s"Input data does not include column someMissingColumn!"))
        assert(resultC.metric.isDefined)
      }

    "execute value picker on the analysis result value, if provided" in
      withSparkSession { sparkSession =>


      val df = getDfMissing(sparkSession)

      // Analysis result should equal to 100.0 for an existing column
      assert(calculate(StatisticConstraint[Double, Double](
        Completeness("item"), _ == 2.0, Some(valueDoubler)), df).status ==
        ConstraintStatus.Success)

      assert(calculate(StatisticConstraint[Double, Double](
        Completeness("item"), _ != 2.0, Some(valueDoubler)), df).status ==
        ConstraintStatus.Failure)

      // Analysis should fail for a non existing column
      assert(calculate(StatisticConstraint[Double, Double](
        Completeness("someMissingColumn"), _ == 2.0, Some(valueDoubler)), df).status ==
        ConstraintStatus.Failure)
      }

    "get the analysis from the context, if provided" in withSparkSession { sparkSession =>
      val df = getDfMissing(sparkSession)

      val emptyResults = Map.empty[Statistic, Metric[_]]
      val validResults = Map[Statistic, Metric[_]](
        Completeness("item") -> calculateSingle(Completeness("item"), df),
        Completeness("someMissingColumn") -> calculateSingle(Completeness("someMissingColumn"), df)
      )

      // Analysis result should equal to 1.0 for an existing column
      assert(StatisticConstraint[Double, Double](Completeness("item"), _ == 1.0)
        .evaluate(validResults).status == ConstraintStatus.Success)
      assert(StatisticConstraint[Double, Double](Completeness("item"), _ != 1.0)
        .evaluate(validResults).status == ConstraintStatus.Failure)
      assert(StatisticConstraint[Double, Double](
          Completeness("someMissingColumn"), _ != 1.0)
        .evaluate(validResults).status == ConstraintStatus.Failure)

      // Although assertion would pass, since analysis result is missing,
      // constraint fails with missing analysis message
      StatisticConstraint[Double, Double](Completeness("item"), _ == 1.0)
        .evaluate(emptyResults) match {
        case result =>
          assert(result.status == ConstraintStatus.Failure)
          assert(result.message.contains("Missing Analysis, can't run the constraint!"))
          assert(result.metric.isEmpty)
      }
    }

    "execute value picker on the analysis result value retrieved from context, if provided" in
      withSparkSession { sparkSession =>
        val df = getDfMissing(sparkSession)
        val validResults = Map[Statistic, Metric[_]](
          Completeness("item") -> calculateSingle(Completeness("item"), df))

        assert(StatisticConstraint[Double, Double](
            Completeness("item"), _ == 2.0, Some(valueDoubler))
          .evaluate(validResults).status == ConstraintStatus.Success)
      }


    "fail on analysis if value picker is provided but fails" in withSparkSession { sparkSession =>
      def problematicValuePicker(value: Double): Double = {
        throw new RuntimeException("Something wrong with this picker")
      }

      val df = getDfMissing(sparkSession)

      val emptyResults = Map.empty[Statistic, Metric[_]]
      val validResults = Map[Statistic, Metric[_]](
        Completeness("item") -> calculateSingle(Completeness("item"), df))

      val constraint = StatisticConstraint[Double, Double](
          Completeness("item"), _ == 1.0, Some(problematicValuePicker))

      calculate(constraint, df) match {
        case result =>
          assert(result.status == ConstraintStatus.Failure)
          assert(result.message.get.contains("Can't retrieve the value to assert on"))
          assert(result.metric.isDefined)
      }

      constraint.evaluate(validResults) match {
        case result =>
          assert(result.status == ConstraintStatus.Failure)
          assert(result.message.isDefined)
          assert(result.message.get.startsWith("Can't retrieve the value to assert on"))
          assert(result.metric.isDefined)
      }

      constraint.evaluate(emptyResults) match {
        case result =>
          assert(result.status == ConstraintStatus.Failure)
          assert(result.message.contains("Missing Analysis, can't run the constraint!"))
          assert(result.metric.isEmpty)
      }

    }

    "fail on failed assertion function with hint in exception message if provided" in
      withSparkSession { sparkSession =>

      val df = getDfMissing(sparkSession)

      val failingConstraint = StatisticConstraint[Double, Double](
          Completeness("item"), _ == 0.9, hint = Some("Value should be like ...!"))

      calculate(failingConstraint, df) match {
        case result =>
          assert(result.status == ConstraintStatus.Failure)
          assert(result.message.isDefined)
          assert(result.message.get == "Value: 1.0 does not meet the constraint requirement! " +
            "Value should be like ...!")
          assert(result.metric.isDefined)
      }
    }

    "return failed constraint for a failing assertion" in withSparkSession { session =>
      val msg = "-test-"
      val exception = new RuntimeException(msg)
      val df = getDfMissing(session)

      def failingAssertion(value: Double): Boolean = throw exception

      val constraintResult = calculate(
        StatisticConstraint[Double, Double](
          Completeness("item"), failingAssertion), df
      )

      assert(constraintResult.status == ConstraintStatus.Failure)
      assert(constraintResult.metric.isDefined)
      assert(constraintResult.message.contains(s"Can't execute the assertion: $msg!"))
    }

  }



}
