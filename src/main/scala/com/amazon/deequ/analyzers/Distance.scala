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
import com.amazon.deequ.analyzers.CategoricalDistanceMethod.{CategoricalDistanceMethod, Chisquare, LInfinity}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.Statistics._
import org.apache.spark.mllib.stat.test.ChiSqTestResult

object CategoricalDistanceMethod extends Enumeration {
  type CategoricalDistanceMethod = Value
  val LInfinity, Chisquare = Value
}

object Distance {

    /** Calculate distance of numerical profiles based on KLL Sketches and L-Infinity Distance */
    def numericalDistance(
      sample1: QuantileNonSample[Double],
      sample2: QuantileNonSample[Double],
      correctForLowNumberOfSamples: Boolean = false)
    : Double = {
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
      selectMetrics(linfSimple, n, m, correctForLowNumberOfSamples)
    }

  /** Calculate distance of categorical profiles based on different distance methods
   *
   * Thresholds for chi-square method:
   *    - for 2x2 tables: all expected counts should be 10 or greater (Cochran, William G. "The χ2 test of goodness of fit." The Annals of mathematical statistics (1952): 315-345.)
   *    - for tables larger than 2 x 2: "No more than 20% of the expected counts are less than 5 and all individual expected counts are 1 or greater" (Yates, Moore & McCabe, 1999, The Practice of Statistics, p. 734)
   *
   * @param sample1                      the mapping between categories(keys) and counts(values) of the observed sample
   * @param sample2                      the mapping between categories(keys) and counts(values) of the expected baseline
   * @param correctForLowNumberOfSamples if true returns chi-square statistics otherwise p-value
   * @param method                       Method to use: LInfinity or Chisquare
   * @param absThresholdYates            Yates absolute threshold for tables larger than 2x2
   * @param percThresholdYates           Yates percentage of categories that can be below threshold for tables larger than 2x2
   * @param absThresholdCochran          Cochran absolute threshold for 2x2 tables
   * @return distance                    can be an absolute distance or a p-value based on the correctForLowNumberOfSamples argument
   */
  def categoricalDistance(
    sample1: scala.collection.mutable.Map[String, Long],
    sample2: scala.collection.mutable.Map[String, Long],
    correctForLowNumberOfSamples: Boolean = false,
    method: CategoricalDistanceMethod = LInfinity,
    absThresholdYates: Integer = 5,
    percThresholdYates: Double = 0.2,
    absThresholdCochran: Integer = 10)
    : Double = {
    method match {
      case LInfinity => categoricalLInfinityDistance(sample1, sample2, correctForLowNumberOfSamples)
      case Chisquare => categoricalChiSquareTest(sample1, sample2, correctForLowNumberOfSamples, absThresholdYates , percThresholdYates,absThresholdCochran )
    }
  }

  /** Calculate distance of categorical profiles based on Chisquare test or stats
   *
   *  for 2x2 tables: all expected counts should be 10 or greater (Cochran, William G. "The χ2 test of goodness of fit." The Annals of mathematical statistics (1952): 315-345.)
   *  for tables larger than 2 x 2: "No more than 20% of the expected counts are less than 5 and all individual expected counts are 1 or greater" (Yates, Moore & McCabe, 1999, The Practice of Statistics, p. 734)
   *
   *  @param sample                       the mapping between categories(keys) and counts(values) of the observed sample
   *  @param expected                     the mapping between categories(keys) and counts(values) of the expected baseline
   *  @param correctForLowNumberOfSamples if true returns chi-square statistics otherwise p-value
   *  @param absThresholdYates            Yates absolute threshold for tables larger than 2x2
   *  @param percThresholdYates           Yates percentage of categories that can be below threshold for tables larger than 2x2
   *  @param absThresholdCochran          Cochran absolute threshold for 2x2 tables
   *  @return distance                    can be an absolute distance or a p-value based on the correctForLowNumberOfSamples argument
   *
   */
  private[this] def categoricalChiSquareTest(
    sample: scala.collection.mutable.Map[String, Long],
    expected: scala.collection.mutable.Map[String, Long],
    correctForLowNumberOfSamples: Boolean = false,
    absThresholdYates : Integer = 5 ,
    percThresholdYates : Double = 0.2,
    absThresholdCochran : Integer = 10)
  : Double = {

    val sample_sum: Double = sample.filter(e => expected.contains(e._1)).map((e => e._2)).sum
    val expected_sum: Double = expected.map(e => e._2).sum

    // Normalize the expected input
    val expectedNorm: scala.collection.mutable.Map[String, Double] = expected.map(e => (e._1, (e._2 / expected_sum * sample_sum)))

    // Call the function that regroups categories if necessary depending on thresholds
    val (regrouped_sample, regrouped_expected) = regroupCategories(sample.map(e => (e._1, e._2.toDouble)), expectedNorm, absThresholdYates, percThresholdYates, absThresholdCochran)

    // If less than 2 categories remain we cannot conduct the test
    if (regrouped_sample.keySet.size < 2) {
      Double.NaN
    } else {
      // run chi-square test and return statistics or p-value
      val result = chiSquareTest(regrouped_sample, regrouped_expected)
      if (correctForLowNumberOfSamples) {
        result.statistic
      } else {
        result.pValue
      }
    }
  }

  /** Regroup categories with elements below threshold, required for chi-square test
   *
   * for 2x2 tables: all expected counts should be 10 or greater (Cochran, William G. "The χ2 test of goodness of fit." The Annals of mathematical statistics (1952): 315-345.)
   * for tables larger than 2 x 2: "No more than 20% of the expected counts are less than 5 and all individual expected counts are 1 or greater" (Yates, Moore & McCabe, 1999, The Practice of Statistics, p. 734)
   *
   * @param sample                       the mapping between categories(keys) and counts(values) of the observed sample
   * @param expected                     the mapping between categories(keys) and counts(values) of the expected baseline
   * @param absThresholdYates            Yates absolute threshold for tables larger than 2x2
   * @param percThresholdYates           Yates percentage of categories that can be below threshold for tables larger than 2x2
   * @param absThresholdCochran          Cochran absolute threshold for 2x2 tables
   * @return (sample, expected)          returns the two regrouped mappings
   *
   */
  private[this] def regroupCategories(
    sample: scala.collection.mutable.Map[String, Double],
    expected: scala.collection.mutable.Map[String, Double],
    absThresholdYates: Integer = 5,
    percThresholdYates: Double = 0.2,
    absThresholdCochran: Integer = 10)
    : (scala.collection.mutable.Map[String, Double], scala.collection.mutable.Map[String, Double]) = {

    // If number of categories is below return original mappings
    if (expected.keySet.size < 2) {
      (sample, expected)
    } else {
      // Determine thresholds depending on dimensions of mapping (2x2 tables use Cochran, all other tables Yates thresholds)
      var absThresholdPerColumn : Integer = absThresholdCochran
      var maxNbColumnsBelowThreshold: Integer = 0
      if (expected.keySet.size > 2) {
        absThresholdPerColumn = absThresholdYates
        maxNbColumnsBelowThreshold = (percThresholdYates * expected.keySet.size).toInt
      }
      // Count number of categories below threshold
      val nbExpectedColumnsBelowThreshold = expected.filter(e => e._2 < absThresholdPerColumn).keySet.size

      // If the number of categories below threshold exceeds the authorized maximum, small categories are regrouped until valid
      if (nbExpectedColumnsBelowThreshold > maxNbColumnsBelowThreshold){

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
   * @return ChiSqTestResult    returns the chi-square test result object (contains both statistics and p-value)
   *
   */
  private[this] def chiSquareTest(
                                   sample: scala.collection.mutable.Map[String, Double],
                                   expected: scala.collection.mutable.Map[String, Double])
  : ChiSqTestResult = {

    var sample_array = Array[Double]()
    var expected_array = Array[Double]()

    expected.keySet.foreach { key =>
      val cdf1: Double = sample.getOrElse(key, 0.0)
      val cdf2: Double = expected(key)
      sample_array = sample_array :+ cdf1
      expected_array = expected_array :+ cdf2
    }

    val vec_sample: Vector = Vectors.dense(sample_array)
    val vec_expected: Vector = Vectors.dense(expected_array)

    Statistics.chiSqTest(vec_sample, vec_expected)
  }

  /** Calculate distance of categorical profiles based on L-Infinity Distance */
  private[this] def categoricalLInfinityDistance(
    sample1: scala.collection.mutable.Map[String, Long],
    sample2: scala.collection.mutable.Map[String, Long],
    correctForLowNumberOfSamples: Boolean = false)
  : Double = {
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
    selectMetrics(linfSimple, n, m, correctForLowNumberOfSamples)
  }

  /** Select which metrics to compute (linf_simple or linf_robust)
   *  based on whether samples are enough */
   private[this] def selectMetrics(
     linfSimple: Double,
     n: Double,
     m: Double,
     correctForLowNumberOfSamples: Boolean = false)
   : Double = {
     if (correctForLowNumberOfSamples) {
       linfSimple
     } else {
       // This formula is based on  “Two-sample Kolmogorov–Smirnov test"
       // Reference: https://en.m.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test
       val linfRobust = Math.max(0.0, linfSimple - 1.8 * Math.sqrt((n + m) / (n * m)))
       linfRobust
     }
   }
}

