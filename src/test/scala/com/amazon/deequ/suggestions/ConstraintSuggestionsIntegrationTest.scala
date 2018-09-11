package com.amazon.deequ.suggestions

import java.util.{Random, UUID}

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers.{Analyzer, Completeness, Compliance, DataType, State, Uniqueness}
import com.amazon.deequ.constraints.AnalysisBasedConstraint
import com.amazon.deequ.metrics.Metric
import org.scalatest.WordSpec

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
      val rng = new Random()

      val categories = Array("DE", "NA", "IN", "EU")

      val records = (0 until numRecords)
        .map { _ =>

          // Unique string id
          val id = UUID.randomUUID().toString
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

      val suggestions = ConstraintSuggestions.suggest(data)

      suggestions.columnProfiles.profiles.foreach { profile =>
        println(profile)
      }

      // IS NOT NULL for "id"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        analyzer == Completeness("id") && assertionFunc(1.0)
      }

      // UNIQUE for "id"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        analyzer == Uniqueness("id") && assertionFunc(1.0)
      }

      // No particular datatype for "id"
      assertNoConstraintExistsIn(suggestions) { (analyzer, _) =>
        analyzer == DataType("id")
      }

      // IS NOT NULL for "marketplace"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        analyzer == Completeness("marketplace") && assertionFunc(1.0)
      }

      // Categorical range for "marketplace"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>

        assertionFunc(1.0) &&
          analyzer.isInstanceOf[Compliance] &&
          analyzer.asInstanceOf[Compliance]
            .instance.startsWith(s"'marketplace' has value range")
      }

      // IS NOT NULL for "measurement"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        analyzer == Completeness("measurement") && assertionFunc(1.0)
      }

      // > 0 for "measurement"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        assertionFunc(1.0) &&
          analyzer.isInstanceOf[Compliance] &&
          analyzer.asInstanceOf[Compliance]
            .instance == "'measurement' has only positive values"
      }

      // No type for "measurement"
      assertNoConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        analyzer == DataType("measurement") && assertionFunc(1.0)
      }

      // IS NOT NULL for "propertyA"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        analyzer == Completeness("propertyA") && assertionFunc(1.0)
      }

      // Boolean type for "measurement"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        // We cannot check which type the constraint looks for unfortunately
        analyzer == DataType("propertyA") && assertionFunc(1.0)
      }

      // IS NOT NULL for "measurement2"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        analyzer == Completeness("measurement2") && assertionFunc(1.0)
      }

      // No range constraints for "measurement2"
      assertNoConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        assertionFunc(1.0) &&
          analyzer.isInstanceOf[Compliance] &&
          analyzer.asInstanceOf[Compliance]
            .instance == "'measurement2' has only positive values"
      }

      // No range constraints for "measurement2"
      assertNoConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        assertionFunc(1.0) &&
          analyzer.isInstanceOf[Compliance] &&
          analyzer.asInstanceOf[Compliance]
            .instance == "'measurement2' has no negative values"
      }

      // Fractional type for "measurement2"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        // We cannot check which type the constraint looks for unfortunately
        analyzer == DataType("measurement2") && assertionFunc(1.0)
      }

      // Bounded completeness for "measurement3"
      assertConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        analyzer == Completeness("measurement3") && assertionFunc(0.8)
      }

      // Bounded completeness for "measurement3"
      assertNoConstraintExistsIn(suggestions) { (analyzer, assertionFunc) =>
        analyzer == Completeness("measurement3") && assertionFunc(0.2)
      }

    }
  }

  private[this] def assertConstraintExistsIn(suggestions: SuggestedConstraints)
      (func: (Analyzer[State[_], Metric[_]], (Double => Boolean)) => Boolean)
    : Unit = {

    assert(evaluate(suggestions, func))
  }

  private[this] def assertNoConstraintExistsIn(suggestions: SuggestedConstraints)
      (func: (Analyzer[State[_], Metric[_]], (Double => Boolean)) => Boolean)
    : Unit = {

    assert(!evaluate(suggestions, func))
  }


  private[this] def evaluate(
      suggestions: SuggestedConstraints,
      func: (Analyzer[State[_], Metric[_]], (Double => Boolean)) => Boolean)
    : Boolean = {

    suggestions.constraints.exists { namedConstraint =>
      val constraint = namedConstraint.inner.asInstanceOf[AnalysisBasedConstraint[_, _, _]]
      val assertionFunction = constraint.assertion.asInstanceOf[(Double => Boolean)]

      func(constraint.analyzer.asInstanceOf[Analyzer[State[_], Metric[_]]], assertionFunction)
    }
  }

}
