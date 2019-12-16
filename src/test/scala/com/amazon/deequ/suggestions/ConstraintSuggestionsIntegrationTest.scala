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

package com.amazon.deequ
package suggestions

import com.amazon.deequ.analyzers._
import com.amazon.deequ.constraints.{AnalysisBasedConstraint, Constraint, ConstraintDecorator}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.suggestions.rules.{NonNegativeNumbersRule, UniqueIfApproximatelyUniqueRule}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.scalatest.WordSpec

import scala.util.Random

case class Record(
    id: String,
    marketplace: String,
    measurement: Double,
    propertyA: String,
    measurement2: String,
    measurement3: String,
    allNullColumn: String,
    allNullColumn2: java.lang.Double
)

class ConstraintSuggestionsIntegrationTest extends WordSpec with SparkContextSpec {

  "Suggestions" should {

    "return expected candidates" in withSparkSession { session =>

      val numRecords = 10000
      val rng = new Random(0)

      val categories = Array("DE", "NA", "IN", "EU")

      val records = (0 until numRecords)
        .map { record =>

          // Unique string id
          val id = s"id$record"
          // Categorial string value
          val marketplace = categories(rng.nextInt(categories.length))
          // Non-negative fractional
          val measurement = rng.nextDouble()
          // Boolean
          val propertyA = rng.nextBoolean().toString
          // negative fractional
          val measurement2 = (rng.nextInt(100).toDouble - 0.5).toString

          // incomplete string
          val measurement3 = rng.nextDouble() match {
            case d: Double if d >= 0.5 => d.toString
            case _ => null
          }

          Record(id, marketplace, measurement, propertyA, measurement2, measurement3, null, null)
        }

      val data = session.createDataFrame(records)

      val constraintSuggestionResult = ConstraintSuggestionRunner()
        .onData(data)
        .addConstraintRules(Rules.DEFAULT)
        .addConstraintRule(UniqueIfApproximatelyUniqueRule())
        .run()

      val columnProfiles = constraintSuggestionResult.columnProfiles.values

      columnProfiles.foreach { profile =>
        println(profile)
      }

      assert(constraintSuggestionResult.numRecordsUsedForProfiling == numRecords)

      // IS NOT NULL for "id"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        analyzer == Completeness("id") && assertionFunc(1.0)
      }

      // UNIQUE for "id"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        analyzer == Uniqueness("id") && assertionFunc(1.0)
      }

      // No particular datatype for "id"
      assertNoConstraintExistsIn(constraintSuggestionResult) { (analyzer, _) =>
        analyzer == DataType("id")
      }

      // IS NOT NULL for "marketplace"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        analyzer == Completeness("marketplace") && assertionFunc(1.0)
      }

      // Categorical range for "marketplace"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>

        assertionFunc(1.0) &&
          analyzer.isInstanceOf[Compliance] &&
          analyzer.asInstanceOf[Compliance]
            .instance.startsWith(s"'marketplace' has value range")
      }

      // IS NOT NULL for "measurement"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        analyzer == Completeness("measurement") && assertionFunc(1.0)
      }

      // > 0 for "measurement"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        assertionFunc(1.0) &&
          analyzer.isInstanceOf[Compliance] &&
          analyzer.asInstanceOf[Compliance]
            .instance == "'measurement' has no negative values"
      }

      // No type for "measurement"
      assertNoConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        analyzer == DataType("measurement") && assertionFunc(1.0)
      }

      // IS NOT NULL for "propertyA"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        analyzer == Completeness("propertyA") && assertionFunc(1.0)
      }

      // Boolean type for "measurement"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        // We cannot check which type the constraint looks for unfortunately
        analyzer == DataType("propertyA") && assertionFunc(1.0)
      }

      // IS NOT NULL for "measurement2"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        analyzer == Completeness("measurement2") && assertionFunc(1.0)
      }

      // No range constraints for "measurement2"
      assertNoConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        assertionFunc(1.0) &&
          analyzer.isInstanceOf[Compliance] &&
          analyzer.asInstanceOf[Compliance]
            .instance == "'measurement2' has only positive values"
      }

      // No range constraints for "measurement2"
      assertNoConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        assertionFunc(1.0) &&
          analyzer.isInstanceOf[Compliance] &&
          analyzer.asInstanceOf[Compliance]
            .instance == "'measurement2' has no negative values"
      }

      // Fractional type for "measurement2"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        // We cannot check which type the constraint looks for unfortunately
        analyzer == DataType("measurement2") && assertionFunc(1.0)
      }

      // Bounded completeness for "measurement3"
      assertConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        analyzer == Completeness("measurement3") && assertionFunc(0.8)
      }

      // Bounded completeness for "measurement3"
      assertNoConstraintExistsIn(constraintSuggestionResult) { (analyzer, assertionFunc) =>
        analyzer == Completeness("measurement3") && assertionFunc(0.2)
      }

    }

    "issue non negativity constraint for positive data" in withSparkSession { sparkSession =>
      val col = "some"
      val data = dataFrameWithColumn(col, IntegerType, sparkSession, Row(0), Row(1), Row(null))

      val results = ConstraintSuggestionRunner()
        .onData(data)
        .addConstraintRules(NonNegativeNumbersRule() :: Nil)
        .run()

      assert(results.constraintSuggestions.size == 1)
    }

    "issue non negativity constraint for data > 0" in withSparkSession { sparkSession =>
      val col = "some"
      val data = dataFrameWithColumn(col, IntegerType, sparkSession, Row(1), Row(null))

      val results = ConstraintSuggestionRunner()
        .onData(data)
        .addConstraintRules(NonNegativeNumbersRule() :: Nil)
        .run()

      assert(results.constraintSuggestions.size == 1)
    }
  }

  private[this] def assertConstraintExistsIn(constraintSuggestionResult: ConstraintSuggestionResult)
      (func: (Analyzer[State[_], Metric[_]], Double => Boolean) => Boolean)
    : Unit = {

    assert(evaluate(constraintSuggestionResult, func))
  }

  private[this] def assertNoConstraintExistsIn(
      constraintSuggestionResult: ConstraintSuggestionResult)(
      func: (Analyzer[State[_], Metric[_]], Double => Boolean) => Boolean)
    : Unit = {

    assert(!evaluate(constraintSuggestionResult, func))
  }


  private[this] def evaluate(
      constraintSuggestionResult: ConstraintSuggestionResult,
      func: (Analyzer[State[_], Metric[_]], Double => Boolean) => Boolean)
    : Boolean = {

    constraintSuggestionResult
      .constraintSuggestions.values.reduce(_ ++ _)
      .map(constraintSuggestion => constraintSuggestion.constraint)
      .map {
        case namedConstraint: ConstraintDecorator => namedConstraint.inner
        case constraint: Constraint => constraint
      }
      .exists { constraint =>
        val analysisBasedConstraint = constraint.asInstanceOf[AnalysisBasedConstraint[_, _, _]]
        val assertionFunction = analysisBasedConstraint.assertion.asInstanceOf[Double => Boolean]

      val analyzer = analysisBasedConstraint.analyzer.asInstanceOf[Analyzer[State[_], Metric[_]]]
        func(analyzer, assertionFunction)
      }
  }

}
