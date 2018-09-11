package com.amazon.deequ.suggestions

import com.amazon.deequ.analyzers.DataTypeInstances
import com.amazon.deequ.metrics.{Distribution, DistributionValue}
import org.scalatest.WordSpec
import DataTypeInstances._

class ConstraintSuggestionsTest extends WordSpec {


  "Suggestion rules" should {

    "be applied correctly for the CompleteIfCompleteRule" in {

      val complete = StandardColumnProfile("col1", 1.0, 100, String, false, Map.empty, None)
      val incomplete = StandardColumnProfile("col1", .25, 100, String, false, Map.empty, None)

      assert(CompleteIfCompleteRule.shouldBeApplied(complete, 1000))
      assert(!CompleteIfCompleteRule.shouldBeApplied(incomplete, 1000))
    }

    "be applied correctly for the RetainCompletenessRule" in {

      val complete = StandardColumnProfile("col1", 1.0, 100, String, false, Map.empty, None)
      val incomplete = StandardColumnProfile("col1", .25, 100, String, false, Map.empty, None)

      assert(!RetainCompletenessRule.shouldBeApplied(complete, 1000))
      assert(RetainCompletenessRule.shouldBeApplied(incomplete, 1000))
    }

    "be applied correctly for the UniqueIfApproximatelyUniqueRule" in {

      val unique = StandardColumnProfile("col1", 1.0, 100, String, false, Map.empty, None)
      val maybeUnique = StandardColumnProfile("col1", 1.0, 95, String, false, Map.empty, None)
      val maybeNonUnique = StandardColumnProfile("col1", 1.0, 91, String, false, Map.empty, None)
      val nonUnique = StandardColumnProfile("col1", 1.0, 20, String, false, Map.empty, None)

      assert(UniqueIfApproximatelyUniqueRule.shouldBeApplied(unique, 100))
      assert(UniqueIfApproximatelyUniqueRule.shouldBeApplied(maybeUnique, 100))
      assert(!UniqueIfApproximatelyUniqueRule.shouldBeApplied(maybeNonUnique, 100))
      assert(!UniqueIfApproximatelyUniqueRule.shouldBeApplied(nonUnique, 100))
    }

    "be applied correctly for the RetainTypeRule" in {

      val string = StandardColumnProfile("col1", 1.0, 100, String, true, Map.empty, None)
      val boolean = StandardColumnProfile("col1", 1.0, 100, Boolean, true, Map.empty, None)
      val fractional = StandardColumnProfile("col1", 1.0, 100, Fractional, true, Map.empty, None)
      val integer = StandardColumnProfile("col1", 1.0, 100, Integral, true, Map.empty, None)
      val unknown = StandardColumnProfile("col1", 1.0, 100, Unknown, true, Map.empty, None)

      val stringNonInferred = StandardColumnProfile("col1", 1.0, 100, String, false, Map.empty,
        None)
      val booleanNonInferred = StandardColumnProfile("col1", 1.0, 100, Boolean, false, Map.empty,
        None)
      val fractionalNonInferred = StandardColumnProfile("col1", 1.0, 100, Fractional, false,
        Map.empty, None)
      val integerNonInferred = StandardColumnProfile("col1", 1.0, 100, Integral, false,
        Map.empty, None)

      assert(!RetainTypeRule.shouldBeApplied(string, 100))
      assert(!RetainTypeRule.shouldBeApplied(unknown, 100))
      assert(!RetainTypeRule.shouldBeApplied(stringNonInferred, 100))
      assert(!RetainTypeRule.shouldBeApplied(booleanNonInferred, 100))
      assert(!RetainTypeRule.shouldBeApplied(fractionalNonInferred, 100))
      assert(!RetainTypeRule.shouldBeApplied(integerNonInferred, 100))

      assert(RetainTypeRule.shouldBeApplied(boolean, 100))
      assert(RetainTypeRule.shouldBeApplied(fractional, 100))
      assert(RetainTypeRule.shouldBeApplied(integer, 100))
    }

    "be applied correctly for the CategoricalRangeRule" in {

      // Ratio of non-unique distinct values must be > 90%
      val nonSkewedDist = Distribution(Map(
        "a" -> DistributionValue(5, 0.0),
        "b" -> DistributionValue(10, 0.0),
        "c" -> DistributionValue(1, 0.0),
        "d" -> DistributionValue(4, 0.0),
        "e" -> DistributionValue(4, 0.0),
        "f" -> DistributionValue(4, 0.0),
        "g" -> DistributionValue(4, 0.0),
        "h" -> DistributionValue(4, 0.0),
        "i" -> DistributionValue(4, 0.0),
        "j" -> DistributionValue(4, 0.0),
        "k" -> DistributionValue(4, 0.0)),
        11)

      val skewedDist = Distribution(Map(
        "a" -> DistributionValue(17, 0.85),
        "b" -> DistributionValue(1, 0.05),
        "c" -> DistributionValue(1, 0.05),
        "d" -> DistributionValue(1, 0.05)),
        4)

      val noDistribution = Distribution(Map.empty, 0)

      val stringWithNonSkewedDist = StandardColumnProfile("col1", 1.0, 100, String, false,
        Map.empty, Some(nonSkewedDist))
      val stringWithSkewedDist = StandardColumnProfile("col1", 1.0, 100, String, false,
        Map.empty, Some(skewedDist))
      val stringNoDist = StandardColumnProfile("col1", 1.0, 95, String, false, Map.empty, None)
      val boolNoDist = StandardColumnProfile("col1", 1.0, 94, Boolean, false, Map.empty, None)
      val boolWithEmptyDist = StandardColumnProfile("col1", 1.0, 20, Boolean, false, Map.empty,
        Some(noDistribution))

      assert(CategoricalRangeRule.shouldBeApplied(stringWithNonSkewedDist, 100))

      assert(!CategoricalRangeRule.shouldBeApplied(stringWithSkewedDist, 100))
      assert(!CategoricalRangeRule.shouldBeApplied(stringNoDist, 100))
      assert(!CategoricalRangeRule.shouldBeApplied(boolNoDist, 100))
      assert(!CategoricalRangeRule.shouldBeApplied(boolWithEmptyDist, 100))
    }

    "be applied correctly for the NonNegativeNumbersRule and PositiveNumbersRule" in {

      val negative = NumericColumnProfile("col1", 1.0, 100, Fractional, false, Map.empty, None,
        Some(10), Some(100), Some(-1.76), Some(1.0), None)
      val zero = NumericColumnProfile("col1", 1.0, 100, Fractional, false, Map.empty, None,
        Some(10), Some(100), Some(0.0), Some(1.0), None)
      val positive = NumericColumnProfile("col1", 1.0, 100, Fractional, false, Map.empty, None,
        Some(10), Some(100), Some(0.05), Some(1.0), None)

      assert(!NonNegativeNumbersRule.shouldBeApplied(negative, 100))
      assert(NonNegativeNumbersRule.shouldBeApplied(zero, 100))
      assert(!NonNegativeNumbersRule.shouldBeApplied(positive, 100))

      assert(!PositiveNumbersRule.shouldBeApplied(negative, 100))
      assert(!PositiveNumbersRule.shouldBeApplied(zero, 100))
      assert(PositiveNumbersRule.shouldBeApplied(positive, 100))
    }
  }

}
