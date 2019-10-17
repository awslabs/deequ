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

import com.amazon.deequ.analyzers.DataTypeInstances
import com.amazon.deequ.analyzers.DataTypeInstances._
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.metrics.{Distribution, DistributionValue}
import com.amazon.deequ.profiles._
import com.amazon.deequ.utils.FixtureSupport
import com.amazon.deequ.{SparkContextSpec, VerificationSuite}
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec

class ConstraintRulesTest extends WordSpec with FixtureSupport with SparkContextSpec
  with MockFactory{


  "CompleteIfCompleteRule" should {
    "be applied correctly" in {

      val complete = StandardColumnProfile("col1", 1.0, 100, String, false, Map.empty, None)
      val incomplete = StandardColumnProfile("col1", .25, 100, String, false, Map.empty, None)

      assert(CompleteIfCompleteRule().shouldBeApplied(complete, 1000))
      assert(!CompleteIfCompleteRule().shouldBeApplied(incomplete, 1000))
    }

    "return evaluable constraint candidates" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

      val fakeColumnProfile = getFakeColumnProfileWithNameAndCompleteness("att1", 1)

      val check = Check(CheckLevel.Warning, "some")
        .addConstraint(CompleteIfCompleteRule().candidate(fakeColumnProfile, 100).constraint)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }

    "return working code to add constraint to check" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

      val fakeColumnProfile = getFakeColumnProfileWithNameAndCompleteness("att1", 1)

      val codeForConstraint = CompleteIfCompleteRule().candidate(fakeColumnProfile, 100)
        .codeForConstraint
      val expectedCodeForConstraint = """.isComplete("att1")"""

      assert(expectedCodeForConstraint == codeForConstraint)

      val check = Check(CheckLevel.Warning, "some")
        .isComplete("att1")

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }
  }

  "RetainCompletenessRule" should {
    "be applied correctly" in {

      val complete = StandardColumnProfile("col1", 1.0, 100, String, false, Map.empty, None)
      val incomplete = StandardColumnProfile("col1", .25, 100, String, false, Map.empty, None)

      assert(!RetainCompletenessRule().shouldBeApplied(complete, 1000))
      assert(RetainCompletenessRule().shouldBeApplied(incomplete, 1000))
    }

    "return evaluable constraint candidates" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

      val fakeColumnProfile = getFakeColumnProfileWithNameAndCompleteness("att1", 0.5)

      val check = Check(CheckLevel.Warning, "some")
        .addConstraint(RetainCompletenessRule().candidate(fakeColumnProfile, 100).constraint)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }

    "return working code to add constraint to check" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

      val fakeColumnProfile = getFakeColumnProfileWithNameAndCompleteness("att1", 0.5)

      val codeForConstraint = RetainCompletenessRule().candidate(fakeColumnProfile, 100)
        .codeForConstraint

      val expectedCodeForConstraint = """.hasCompleteness("att1", _ >= 0.4,
          | Some("It should be above 0.4!"))""".stripMargin.replaceAll("\n", "")

      assert(expectedCodeForConstraint == codeForConstraint)

      val check = Check(CheckLevel.Warning, "some")
        .hasCompleteness("att1", _ >= 0.4, Some("It should be above 0.4!"))

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }
  }

  "UniqueIfApproximatelyUniqueRule" should {
    "be applied correctly" in {

      val unique = StandardColumnProfile("col1", 1.0, 100, String, false, Map.empty, None)
      val maybeUnique = StandardColumnProfile("col1", 1.0, 95, String, false, Map.empty, None)
      val maybeNonUnique = StandardColumnProfile("col1", 1.0, 91, String, false, Map.empty, None)
      val nonUnique = StandardColumnProfile("col1", 1.0, 20, String, false, Map.empty, None)

      assert(UniqueIfApproximatelyUniqueRule().shouldBeApplied(unique, 100))
      assert(UniqueIfApproximatelyUniqueRule().shouldBeApplied(maybeUnique, 100))
      assert(!UniqueIfApproximatelyUniqueRule().shouldBeApplied(maybeNonUnique, 100))
      assert(!UniqueIfApproximatelyUniqueRule().shouldBeApplied(nonUnique, 100))
    }

    "return evaluable constraint candidates" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

      val fakeColumnProfile = getFakeColumnProfileWithNameAndApproxNumDistinctValues("item", 100)

      val check = Check(CheckLevel.Warning, "some")
        .addConstraint(UniqueIfApproximatelyUniqueRule().candidate(fakeColumnProfile, 100)
          .constraint)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }

    "return working code to add constraint to check" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

      val fakeColumnProfile = getFakeColumnProfileWithNameAndApproxNumDistinctValues("item", 100)

      val codeForConstraint = UniqueIfApproximatelyUniqueRule().candidate(fakeColumnProfile, 100)
        .codeForConstraint

      val expectedCodeForConstraint = """.isUnique("item")"""

      assert(expectedCodeForConstraint == codeForConstraint)

      val check = Check(CheckLevel.Warning, "some")
        .isUnique("item")

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }
  }

  "RetainTypeRule" should {
    "be applied correctly" in {

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

      assert(!RetainTypeRule().shouldBeApplied(string, 100))
      assert(!RetainTypeRule().shouldBeApplied(unknown, 100))
      assert(!RetainTypeRule().shouldBeApplied(stringNonInferred, 100))
      assert(!RetainTypeRule().shouldBeApplied(booleanNonInferred, 100))
      assert(!RetainTypeRule().shouldBeApplied(fractionalNonInferred, 100))
      assert(!RetainTypeRule().shouldBeApplied(integerNonInferred, 100))

      assert(RetainTypeRule().shouldBeApplied(boolean, 100))
      assert(RetainTypeRule().shouldBeApplied(fractional, 100))
      assert(RetainTypeRule().shouldBeApplied(integer, 100))
    }

    "return evaluable constraint candidates" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

     val fakeColumnProfile = getFakeColumnProfileWithColumnNameAndDataType("item",
       DataTypeInstances.Integral)

      val check = Check(CheckLevel.Warning, "some")
        .addConstraint(RetainTypeRule().candidate(fakeColumnProfile, 100).constraint)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }

    "return working code to add constraint to check" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

      val fakeColumnProfile = getFakeColumnProfileWithColumnNameAndDataType("item",
       DataTypeInstances.Integral)

      val codeForConstraint = RetainTypeRule().candidate(fakeColumnProfile, 100)
        .codeForConstraint

      val expectedCodeForConstraint = """.hasDataType("item", ConstrainableDataTypes.Integral)"""

      assert(expectedCodeForConstraint == codeForConstraint)

      val check = Check(CheckLevel.Warning, "some")
        .hasDataType("item", ConstrainableDataTypes.Integral)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }
  }

  "CategoricalRangeRule" should {
    "be applied correctly" in {

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

      assert(CategoricalRangeRule().shouldBeApplied(stringWithNonSkewedDist, 100))

      assert(!CategoricalRangeRule().shouldBeApplied(stringWithSkewedDist, 100))
      assert(!CategoricalRangeRule().shouldBeApplied(stringNoDist, 100))
      assert(!CategoricalRangeRule().shouldBeApplied(boolNoDist, 100))
      assert(!CategoricalRangeRule().shouldBeApplied(boolWithEmptyDist, 100))
    }

    "return evaluable constraint candidates" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfWithCategoricalColumn(session, 10,
        Seq("'_[a_[]}!@'", "_b%%__"))

      val distribution = Distribution(Map(
        "'_[a_[]}!@'" -> DistributionValue(4, 0.4),
        "_b%%__" -> DistributionValue(6, 0.6)),
        10)

      val fakeColumnProfile = getFakeColumnProfileWithColumnNameAndHistogram("categoricalColumn",
        Some(distribution))

      val check = Check(CheckLevel.Warning, "some")
        .addConstraint(CategoricalRangeRule().candidate(fakeColumnProfile, 100).constraint)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }

    "CategoricalRangeRule should return evaluable constraint candidates even " +
      "if category names contain potentially problematic characters" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfWithCategoricalColumn(session, 10,
        Seq("'_[a_[]}!@'", "_b%%__"))

      val distribution = Distribution(Map(
        "'_[a_[]}!@'" -> DistributionValue(4, 0.4),
        "_b%%__" -> DistributionValue(6, 0.6)),
        10)

      val fakeColumnProfile = getFakeColumnProfileWithColumnNameAndHistogram("categoricalColumn",
        Some(distribution))

      val check = Check(CheckLevel.Warning, "some")
        .addConstraint(CategoricalRangeRule().candidate(fakeColumnProfile, 100).constraint)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }

    "return working code to add constraint to check" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfWithCategoricalColumn(session, 10,
        Seq("'_[a_[]}!@'", "_b%%__"))

      val distribution = Distribution(Map(
        "'_[a_[]}!@'" -> DistributionValue(4, 0.4),
        "_b%%__" -> DistributionValue(6, 0.6)),
        10)

      val fakeColumnProfile = getFakeColumnProfileWithColumnNameAndHistogram("categoricalColumn",
        Some(distribution))

      val codeForConstraint = CategoricalRangeRule().candidate(fakeColumnProfile, 100)
        .codeForConstraint

      val expectedCodeForConstraint = ".isContainedIn(\"categoricalColumn\", " +
        "Array(\"_b%%__\", \"'_[a_[]}!@'\"))"

      assert(expectedCodeForConstraint == codeForConstraint)

      val check = Check(CheckLevel.Warning, "some")
        .isContainedIn("categoricalColumn", Array("_b%%__", "'_[a_[]}!@'"))

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }
  }

  "FractionalCategoricalRangeRule" should {
    "be applied correctly" in {

      // Ratio of non-unique distinct values must be > 90%
      val nonSkewedDistWithFractionalCategoricalRange = Distribution(Map(
        "Y" -> DistributionValue(42, 0.42),
        "'Y'" -> DistributionValue(1, 0.01),
        "N" -> DistributionValue(57, 0.57)),
        3)

      val nonSkewedDistWithActualCategoricalRange = Distribution(Map(
        "Y" -> DistributionValue(5, 0.4),
        "N" -> DistributionValue(10, 0.6)),
        2)

      val somewhatSkewedDist = Distribution(Map(
        "a" -> DistributionValue(85, 0.85),
        "b" -> DistributionValue(7, 0.07),
        "c" -> DistributionValue(2, 0.07),
        "d" -> DistributionValue(1, 0.01)),
        4)

      val skewedDist = Distribution(Map(
        "a" -> DistributionValue(17, 0.79),
        "b" -> DistributionValue(1, 0.07),
        "c" -> DistributionValue(1, 0.07),
        "d" -> DistributionValue(1, 0.07)),
        4)

      val noDistribution = Distribution(Map.empty, 0)

      val stringWithNonSkewedDistWithFractionalCategoricalRange = StandardColumnProfile("col1", 1.0,
        100, String, false, Map.empty, Some(nonSkewedDistWithFractionalCategoricalRange))
      val stringWithNonSkewedDistWithActualCategoricalRange = StandardColumnProfile("col1", 1.0,
        100, String, false, Map.empty, Some(nonSkewedDistWithActualCategoricalRange))
      val stringWithSomewhatSkewedDist = StandardColumnProfile("col1", 1.0, 100, String, false,
        Map.empty, Some(somewhatSkewedDist))
      val stringWithSkewedDist = StandardColumnProfile("col1", 1.0, 100, String, false,
        Map.empty, Some(skewedDist))
      val stringNoDist = StandardColumnProfile("col1", 1.0, 95, String, false, Map.empty, None)
      val boolNoDist = StandardColumnProfile("col1", 1.0, 94, Boolean, false, Map.empty, None)
      val boolWithEmptyDist = StandardColumnProfile("col1", 1.0, 20, Boolean, false, Map.empty,
        Some(noDistribution))


      assert(FractionalCategoricalRangeRule().shouldBeApplied(stringWithSomewhatSkewedDist, 100))
      assert(FractionalCategoricalRangeRule().shouldBeApplied(
        stringWithNonSkewedDistWithFractionalCategoricalRange, 100))

      assert(!FractionalCategoricalRangeRule().shouldBeApplied(stringWithSkewedDist, 100))
      assert(!FractionalCategoricalRangeRule().shouldBeApplied(
        stringWithNonSkewedDistWithActualCategoricalRange, 100))
      assert(!FractionalCategoricalRangeRule().shouldBeApplied(stringNoDist, 100))
      assert(!FractionalCategoricalRangeRule().shouldBeApplied(boolNoDist, 100))
      assert(!FractionalCategoricalRangeRule().shouldBeApplied(boolWithEmptyDist, 100))
    }

    "return evaluable constraint candidates" in
      withSparkSession { session =>

       val dfWithColumnCandidate = getDfWithCategoricalColumn(session, 10,
        Seq("'_[a_[]}!@'", "_b%%__"))

      val distribution = Distribution(Map(
        "'_[a_[]}!@'" -> DistributionValue(4, 0.4),
        "_b%%__" -> DistributionValue(6, 0.6)),
        10)

      val fakeColumnProfile = getFakeColumnProfileWithColumnNameAndHistogram("categoricalColumn",
        Some(distribution))

      val check = Check(CheckLevel.Warning, "some")
        .addConstraint(FractionalCategoricalRangeRule().candidate(fakeColumnProfile, 100)
          .constraint)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }

    "return evaluable constraint candidates even " +
      "if category names contain potentially problematic characters" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfWithCategoricalColumn(session, 10,
        Seq("'_[a_[]}!@'", "_b%%__"))

      val distribution = Distribution(Map(
        "'_[a_[]}!@'" -> DistributionValue(6, 0.3),
        "_b%%__" -> DistributionValue(13, 0.65),
        "_b%__" -> DistributionValue(1, 0.05)),
        20)

      val fakeColumnProfile = getFakeColumnProfileWithColumnNameAndHistogram("categoricalColumn",
        Some(distribution))

      val check = Check(CheckLevel.Warning, "some")
        .addConstraint(FractionalCategoricalRangeRule().candidate(fakeColumnProfile, 100)
          .constraint)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }

    "return working code to add constraint to check" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfWithCategoricalColumn(session, 10,
        Seq("'_[a_[]}!@'", "_b%%__"))

      val distribution = Distribution(Map(
        "'_[a_[]}!@'" -> DistributionValue(6, 0.3),
        "_b%%__" -> DistributionValue(13, 0.65),
        "_b%__" -> DistributionValue(1, 0.05)),
        20)

      val fakeColumnProfile = getFakeColumnProfileWithColumnNameAndHistogram("categoricalColumn",
        Some(distribution))

      val codeForConstraint = FractionalCategoricalRangeRule().candidate(fakeColumnProfile, 100)
        .codeForConstraint

      val expectedCodeForConstraint = ".isContainedIn(\"categoricalColumn\", Array(\"_b%%__\"," +
        " \"'_[a_[]}!@'\"), _ >= 0.9, Some(\"It should be above 0.9!\"))"

      assert(expectedCodeForConstraint == codeForConstraint)

      val check = Check(CheckLevel.Warning, "some")
        .isContainedIn(
          "categoricalColumn",
          Array("_b%%__", "'_[a_[]}!@'", "_b%__"),
          _ >= 0.9,
          Some("It should be above 0.9!"))

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResult = verificationResult.metrics.head._2

      assert(metricResult.value.isSuccess)
    }
  }

  "NonNegativeNumbersRule and PositiveNumbersRule" should {
    "be applied correctly" in {
      def columnProfileWithMinimum(minimum: Double): NumericColumnProfile = {
        NumericColumnProfile("col1", 1.0, 100, Fractional, isDataTypeInferred = false,
          Map.empty, None, None, Some(10), Some(100), Some(minimum), Some(10000), Some(1.0), None)
      }

      val nRecords = 100

      val negative = columnProfileWithMinimum(-1.76)
      val zero = columnProfileWithMinimum(0.0)
      val positive = columnProfileWithMinimum(0.05)

      assert(!NonNegativeNumbersRule().shouldBeApplied(negative, nRecords))
      assert(NonNegativeNumbersRule().shouldBeApplied(zero, nRecords))
      assert(NonNegativeNumbersRule().shouldBeApplied(positive, nRecords))
    }

    "return evaluable constraint candidates" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

      val fakeColumnProfile = getFakeColumnProfileWithName("item")

      val check = Check(CheckLevel.Warning, "some")
        .addConstraint(NonNegativeNumbersRule().candidate(fakeColumnProfile, 100).constraint)

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResultNonNegativeRule = verificationResult.metrics.head._2

      assert(metricResultNonNegativeRule.value.isSuccess)
    }

    "return working code to add constraint to check" in
      withSparkSession { session =>

      val dfWithColumnCandidate = getDfFull(session)

      val fakeColumnProfile = getFakeColumnProfileWithName("item")

      val codeForNonNegativeConstraint = NonNegativeNumbersRule().candidate(fakeColumnProfile, 100)
        .codeForConstraint
      val expectedCodeForNonNegativeConstraint = ".isNonNegative(\"item\")"
      assert(expectedCodeForNonNegativeConstraint == codeForNonNegativeConstraint)

      val check = Check(CheckLevel.Warning, "some")
        .isNonNegative("item")
        .isPositive("item")

      val verificationResult = VerificationSuite()
        .onData(dfWithColumnCandidate)
        .addCheck(check)
        .run()

      val metricResultNonNegativeRule = verificationResult.metrics.head._2
      val metricResultPositiveRule = verificationResult.metrics.toSeq(1)._2

      assert(metricResultNonNegativeRule.value.isSuccess)
      assert(metricResultPositiveRule.value.isSuccess)
    }
  }

  private[this] def getFakeColumnProfileWithName(columnName: String): ColumnProfile = {

    val fakeColumnProfile = mock[ColumnProfile]
    inSequence {
      (fakeColumnProfile.column _)
        .expects()
        .returns(columnName)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }

  private[this] def getFakeColumnProfileWithNameAndCompleteness(
      columnName: String,
      completeness: Double)
    : ColumnProfile = {

    val fakeColumnProfile = getFakeColumnProfileWithName(columnName)
    inSequence {
      (fakeColumnProfile.completeness _)
        .expects()
        .returns(completeness)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }

  private[this] def getFakeColumnProfileWithNameAndApproxNumDistinctValues(
      columnName: String,
      approximateNumDistinctValues: Long)
    : ColumnProfile = {

    val fakeColumnProfile = getFakeColumnProfileWithName(columnName)
    inSequence {
      (fakeColumnProfile.approximateNumDistinctValues _)
        .expects()
        .returns(approximateNumDistinctValues)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }

  private[this] def getFakeColumnProfileWithColumnNameAndDataType(
      columnName: String,
      dataType: DataTypeInstances.Value)
    : ColumnProfile = {

    val fakeColumnProfile = getFakeColumnProfileWithName(columnName)
    inSequence {
      (fakeColumnProfile.dataType _)
        .expects()
        .returns(dataType)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }

  private[this] def getFakeColumnProfileWithColumnNameAndHistogram(
      columnName: String,
      histogram: Option[Distribution])
    : ColumnProfile = {

    val fakeColumnProfile = getFakeColumnProfileWithName(columnName)
    inSequence {
      (fakeColumnProfile.histogram _)
        .expects()
        .returns(histogram)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }

}
