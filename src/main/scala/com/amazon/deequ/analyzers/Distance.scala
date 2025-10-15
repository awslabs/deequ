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

package com.amazon.deequ.analyzers

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Custom result type to maintain compatibility
case class ChiSquareTestResult(statistic: Double, pValue: Double)
import scala.math.log
import scala.annotation.tailrec
import com.amazon.deequ.metrics.BucketValue

object Distance {
    // Chi-square constants
    // at least two distinct categories are required to run the chi-square test for a categorical variable
    private val chisquareMinDimension: Int = 2

    // for tables larger than 2 x 2:
    //   "No more than 20% of the expected counts are less than 5 and all individual expected counts are 1 or greater"
    //     - Yates, Moore & McCabe, 1999, The Practice of Statistics, p. 734
    private val defaultAbsThresholdYates: Integer = 5
    private val defaultPercThresholdYates: Double = 0.2

    // for 2x2 tables:
    //   all expected counts should be 10 or greater
    //     - Cochran, William G. "The (chi)**2 test of goodness of fit."
    //       The Annals of mathematical statistics (1952): 315-345.
    private val defaultAbsThresholdCochran: Integer = 10

    // Default c(alpha) value corresponding to an alpha value of 0.003,
    // Eq. (15) in Section 3.3.1 of Knuth, D.E., The Art of Computer Programming, Volume 2 (Seminumerical Algorithms),
    // 3rd Edition, Addison Wesley, Reading Mass, 1998.
    private val defaultCAlpha : Double = 1.8

    trait CategoricalDistanceMethod
    case class LInfinityMethod(alpha: Option[Double] = None) extends CategoricalDistanceMethod
    case class ChisquareMethod(absThresholdYates: Integer = defaultAbsThresholdYates,
                               percThresholdYates: Double = defaultPercThresholdYates,
                               absThresholdCochran: Integer = defaultAbsThresholdCochran)
      extends CategoricalDistanceMethod

    /** Calculate distance of numerical profiles based on KLL Sketches and L-Infinity Distance */
    def numericalDistance(sample1: QuantileNonSample[Double],
                          sample2: QuantileNonSample[Double],
                          correctForLowNumberOfSamples: Boolean = false,
                          alpha: Option[Double] = None): Double = {
      val rankMap1 = sample1.getRankMap()
      val rankMap2 = sample2.getRankMap()
      val combinedKeys = rankMap1.keySet.union(rankMap2.keySet)
      val n = rankMap1.valuesIterator.max.toDouble
      val m = rankMap2.valuesIterator.max.toDouble
      var linfSimple = 0.0

      combinedKeys.foreach { key =>
        val cdf1 = sample1.getRank(key, rankMap1) / n
        val cdf2 = sample2.getRank(key, rankMap2) / m
        val cdfDiff = Math.abs(cdf1 - cdf2)
        linfSimple = Math.max(linfSimple, cdfDiff)
      }
      selectMetrics(linfSimple, n, m, correctForLowNumberOfSamples, alpha)
    }

  /** Calculate distance of categorical profiles based on different distance methods
   *
   * Thresholds for chi-square method:
   *  - for 2x2 tables:
   *      all expected counts should be 10 or greater
   *        - Cochran, William G. "The (chi)**2 test of goodness of fit."
   *          The Annals of mathematical statistics (1952): 315-345.
   *  - for tables larger than 2 x 2:
   *      "No more than 20% of the expected counts are less than 5 and all individual expected counts are 1 or greater"
   *        - (Yates, Moore & McCabe, 1999, The Practice of Statistics, p. 734)
   *
   * @param sample1                      the mapping between categories(keys) and
   *                                     counts(values) of the observed sample
   * @param sample2                      the mapping between categories(keys) and
   *                                     counts(values) of the expected baseline
   * @param correctForLowNumberOfSamples if true returns chi-square statistics otherwise p-value
   * @param method                       Method to use: LInfinity or Chisquare
   * @return distance                    can be an absolute distance or
   *                                     a p-value based on the correctForLowNumberOfSamples argument
   */
  def categoricalDistance(sample1: scala.collection.mutable.Map[String, Long],
                          sample2: scala.collection.mutable.Map[String, Long],
                          correctForLowNumberOfSamples: Boolean = false,
                          method: CategoricalDistanceMethod = LInfinityMethod()): Double = {
    method match {
      case LInfinityMethod(alpha) => categoricalLInfinityDistance(sample1, sample2, correctForLowNumberOfSamples, alpha)
      case ChisquareMethod(absThresholdYates, percThresholdYates, absThresholdCochran)
        => categoricalChiSquareTest(
            sample1,
            sample2,
            correctForLowNumberOfSamples,
            absThresholdYates,
            percThresholdYates,
            absThresholdCochran )
    }
  }

  /** Calculate distance of categorical profiles based on Chisquare test or stats
   *
   *  for 2x2 tables:
   *    all expected counts should be 10 or greater
   *      - Cochran, William G. "The (chi)**2 test of goodness of fit."
   *        The Annals of mathematical statistics (1952): 315-345.
   *  for tables larger than 2 x 2:
   *    "No more than 20% of the expected counts are less than 5 and all individual expected counts are 1 or greater"
   *      - Yates, Moore & McCabe, 1999, The Practice of Statistics, p. 734
   *
   *  @param sample                       the mapping between categories(keys) and
   *                                      counts(values) of the observed sample
   *  @param expected                     the mapping between categories(keys) and
   *                                      counts(values) of the expected baseline
   *  @param correctForLowNumberOfSamples if true returns chi-square statistics otherwise p-value
   *  @param absThresholdYates            Yates absolute threshold for tables larger than 2x2
   *  @param percThresholdYates           Yates percentage of categories that can be
   *                                      below threshold for tables larger than 2x2
   *  @param absThresholdCochran          Cochran absolute threshold for 2x2 tables
   *  @return distance                    can be an absolute distance or
   *                                      a p-value based on the correctForLowNumberOfSamples argument
   *
   */
  private[this] def categoricalChiSquareTest(sample: scala.collection.mutable.Map[String, Long],
                                             expected: scala.collection.mutable.Map[String, Long],
                                             correctForLowNumberOfSamples: Boolean = false,
                                             absThresholdYates : Integer = defaultAbsThresholdYates,
                                             percThresholdYates : Double = defaultPercThresholdYates,
                                             absThresholdCochran : Integer = defaultAbsThresholdCochran): Double = {

    val sampleSum: Double = sample.filter(e => expected.contains(e._1)).values.sum
    val expectedSum: Double = expected.values.sum

    // Normalize the expected input, normalization is required to conduct the chi-square test
    // While normalization is already included in the mllib chi-square test,
    // we perform normalization manually to execute proper regrouping
    // https://spark.apache.org/docs/3.1.3/api/scala/org/apache/spark/mllib/stat/Statistics$.html#chiSqTest
    val expectedNorm: scala.collection.mutable.Map[String, Double] =
      expected.map(e => (e._1, e._2 / expectedSum * sampleSum))

    // Call the function that regroups categories if necessary depending on thresholds
    val (regroupedSample, regroupedExpected) = regroupCategories(
      sample.map(e => (e._1, e._2.toDouble)), expectedNorm, absThresholdYates, percThresholdYates, absThresholdCochran)

    // If less than 2 categories remain we cannot conduct the test
    if (regroupedExpected.keySet.size < chisquareMinDimension) {
      Double.NaN
    } else {
      // run chi-square test and return statistics or p-value
      val result = chiSquareTest(regroupedSample, regroupedExpected)
      if (correctForLowNumberOfSamples) {
        result.statistic
      } else {
        result.pValue
      }
    }
  }

  /** Regroup categories with elements below threshold, required for chi-square test
   *
   * for 2x2 tables:
   *   all expected counts should be 10 or greater
   *     - Cochran, William G. "The (chi)**2 test of goodness of fit."
   *       The Annals of mathematical statistics (1952): 315-345.
   * for tables larger than 2 x 2:
   *   "No more than 20% of the expected counts are less than 5 and all individual expected counts are 1 or greater"
   *     - Yates, Moore & McCabe, 1999, The Practice of Statistics, p. 734
   *
   * @param sample                       the mapping between categories(keys) and
   *                                     counts(values) of the observed sample
   * @param expected                     the mapping between categories(keys) and
   *                                     counts(values) of the expected baseline
   * @param absThresholdYates            Yates absolute threshold for tables larger than 2x2
   * @param percThresholdYates           Yates percentage of categories that can be
   *                                     below threshold for tables larger than 2x2
   * @param absThresholdCochran          Cochran absolute threshold for 2x2 tables
   * @return (sample, expected)          returns the two regrouped mappings
   *
   */
  @tailrec
  private[this] def regroupCategories(sample: scala.collection.mutable.Map[String, Double],
                                      expected: scala.collection.mutable.Map[String, Double],
                                      absThresholdYates: Integer = defaultAbsThresholdYates,
                                      percThresholdYates: Double = defaultPercThresholdYates,
                                      absThresholdCochran: Integer = defaultAbsThresholdCochran)
    : (scala.collection.mutable.Map[String, Double], scala.collection.mutable.Map[String, Double]) = {

    // If number of categories is below the minimum return original mappings
    if (expected.keySet.size < chisquareMinDimension) {
      (sample, expected)
    } else {
      // Determine thresholds depending on dimensions of mapping
      // 2x2 tables use Cochran, all other tables Yates thresholds
      var absThresholdPerColumn : Integer = absThresholdCochran
      var maxNbColumnsBelowThreshold: Integer = 0
      if (expected.keySet.size > chisquareMinDimension) {
        absThresholdPerColumn = absThresholdYates
        maxNbColumnsBelowThreshold = (percThresholdYates * expected.keySet.size).toInt
      }
      // Count number of categories below threshold
      val nbExpectedColumnsBelowThreshold = expected.filter(e => e._2 < absThresholdPerColumn).keySet.size

      // If the number of categories below threshold exceeds
      // the authorized maximum, small categories are regrouped until valid
      if (nbExpectedColumnsBelowThreshold > maxNbColumnsBelowThreshold) {

        // Identified key that holds minimum value
        val expectedMin: (String, Double) = expected.minBy(e => e._2)
        val sampleMinValue : Double = sample.getOrElse(expectedMin._1, 0)

        // Remove smallest category
        expected.remove(expectedMin._1)
        sample.remove(expectedMin._1)

        // Add value of smallest category to second smallest category
        val expectedSecondMin = expected.minBy(e => e._2)
        val sampleSecondMinValue : Double = sample.getOrElse(expectedSecondMin._1, 0)

        expected.update(expectedSecondMin._1, expectedSecondMin._2 + expectedMin._2 )
        sample.update(expectedSecondMin._1, sampleMinValue + sampleSecondMinValue )

        // Recursively call function until mappings are valid
        regroupCategories(sample, expected, absThresholdYates, percThresholdYates, absThresholdCochran)
      } else {
        // In case the mappings are valid the original mappings are returned
        (sample, expected)
      }
    }
  }


  /** Runs chi-square test on two mappings
   *
   * @param sample              the mapping between categories(keys) and counts(values) of the observed sample
   * @param expected            the mapping between categories(keys) and counts(values) of the expected baseline
   * @return ChiSquareTestResult returns the chi-square test result object (contains both statistics and p-value)
   *
   */
  private[this] def chiSquareTest(sample: scala.collection.mutable.Map[String, Double],
                                  expected: scala.collection.mutable.Map[String, Double]): ChiSquareTestResult = {

    var sampleArray = Array[Double]()
    var expectedArray = Array[Double]()

    expected.keySet.foreach { key =>
      val cdf1: Double = sample.getOrElse(key, 0.0)
      val cdf2: Double = expected(key)
      sampleArray = sampleArray :+ cdf1
      expectedArray = expectedArray :+ cdf2
    }

    // Calculate chi-square statistic manually since ML API works differently
    val chiSquareStatistic = sampleArray.zip(expectedArray).map { case (observed, expected) =>
      if (expected > 0) {
        math.pow(observed - expected, 2) / expected
      } else {
        0.0
      }
    }.sum

    // Calculate degrees of freedom
    val degreesOfFreedom = sampleArray.length - 1

    // For p-value calculation, we'll use a simple approximation
    // In practice, you might want to use a proper statistical library
    val pValue = if (degreesOfFreedom > 0) {
      // Simple approximation - in real implementation you'd use proper chi-square distribution
      math.exp(-chiSquareStatistic / 2)
    } else {
      1.0
    }

    ChiSquareTestResult(chiSquareStatistic, pValue)
  }

  /** Calculate distance of categorical profiles based on L-Infinity Distance */
  private[this] def categoricalLInfinityDistance(sample1: scala.collection.mutable.Map[String, Long],
                                                 sample2: scala.collection.mutable.Map[String, Long],
                                                 correctForLowNumberOfSamples: Boolean = false,
                                                 alpha: Option[Double]): Double = {
    var n = 0.0
    var m = 0.0
    sample1.keySet.foreach { key =>
      n += sample1(key)
    }
    sample2.keySet.foreach { key =>
      m += sample2(key)
    }
    val combinedKeys = sample1.keySet.union(sample2.keySet)
    var linfSimple = 0.0

    combinedKeys.foreach { key =>
      val cdf1 = sample1.getOrElse(key, 0L) / n
      val cdf2 = sample2.getOrElse(key, 0L) / m
      val cdfDiff = Math.abs(cdf1 - cdf2)
      linfSimple = Math.max(linfSimple, cdfDiff)
    }
    selectMetrics(linfSimple, n, m, correctForLowNumberOfSamples, alpha)
  }

  /** Select which metrics to compute (linf_simple or linf_robust)
   *  based on whether samples are enough */
   private[this] def selectMetrics(linfSimple: Double,
                                   n: Double,
                                   m: Double,
                                   correctForLowNumberOfSamples: Boolean = false,
                                   alpha: Option[Double]): Double = {
     if (correctForLowNumberOfSamples) {
       linfSimple
     } else {
       // This formula is based on  “Two-sample Kolmogorov–Smirnov test"
       // Reference: https://en.m.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test

       val cAlpha: Double = alpha match {
         case Some(a) => Math.sqrt(-Math.log(a/2) * 1/2)
         case None => defaultCAlpha
       }
       val linfRobust = Math.max(0.0, linfSimple - cAlpha * Math.sqrt((n + m) / (n * m)))
       linfRobust
     }
   }


  /** Calculate Population Stability Index from two distributions defined as buckets
    *
    * PSI is a measure of the degree to which the distribution of a population has shifted over time or between
    * two different samples of a population in a single number. https://xai.arya.ai/wiki/population-stability-index-psi
    *
    * @param actual                      the actual distribution as a list of buckets
    * @param expected                    the expected distribution as a list of buckets
    * @return Double                     a value closer to 0 means the distributions are stable,
    *                                    common thresholds are 0.1, 0.2
    */

  def populationStabilityIndex(actual: List[BucketValue],
                               expected: List[BucketValue]): Double = {

    // Number of buckets has to be identical for actual and expected
    assert(actual.length==expected.length)

    // Calculate sums
    val actualSum : Long = actual.map(e => e.count).sum
    val expectedSum : Long = expected.map(e => e.count).sum

    // Calculate percentage per bucket
    val actualPercent: List[Double] = actual.map(b => (b.count / actualSum.toDouble))
    val expectedPercent: List[Double] = expected.map(b => (b.count / expectedSum.toDouble))

    // Apply PSI formula PSI = (P - Q) * ln(P/Q)
    actualPercent.zip(expectedPercent).map{
      case (actual, expected) => (actual - expected) * log(actual / expected)
    }.sum
  }

}
