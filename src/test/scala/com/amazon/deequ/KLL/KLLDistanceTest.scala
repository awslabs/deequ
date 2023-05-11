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

package com.amazon.deequ.KLL

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers.Distance.{ChisquareMethod, LInfinityMethod}
import com.amazon.deequ.analyzers.{Distance, QuantileNonSample}
import com.amazon.deequ.metrics.BucketValue
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.WordSpec
import com.amazon.deequ.metrics.{BucketValue}

class KLLDistanceTest extends WordSpec with SparkContextSpec
  with FixtureSupport{

  "KLL distance calculator should compute correct linf_simple" in {
    val sample1 = new QuantileNonSample[Double](4, 0.64)
    val sample2 = new QuantileNonSample[Double](4, 0.64)
    sample1.reconstruct(4, 0.64, Array(Array(1, 2, 3, 4)))
    sample2.reconstruct(4, 0.64, Array(Array(2, 3, 4, 5)))
    val distance = Distance.numericalDistance(sample1, sample2, correctForLowNumberOfSamples = true)
    assert(distance == 0.25)
  }

  "KLL distance calculator should compute correct linf_robust" in {
    val sample1 = new QuantileNonSample[Double](4, 0.64)
    val sample2 = new QuantileNonSample[Double](4, 0.64)
    sample1.reconstruct(4, 0.64, Array(Array(1, 2, 3, 4)))
    sample2.reconstruct(4, 0.64, Array(Array(2, 3, 4, 5)))
    val distance = Distance.numericalDistance(sample1, sample2)
    assert(distance == 0.0)
  }

  "Categorial distance should compute correct linf_simple" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 10L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 5L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 11L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 10L)
    val distance = Distance.categoricalDistance(sample1, sample2, correctForLowNumberOfSamples = true)
    assert(distance == 0.06015037593984962)
  }

  "Categorial distance should compute correct linf_robust" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 10L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 5L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 11L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 10L)
    val distance = Distance.categoricalDistance(sample1, sample2)
    assert(distance == 0.0)
  }

  "Categorial distance should compute correct linf_simple with different bin value" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 10L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 5L)
    val sample2 = scala.collection.mutable.Map(
      "f" -> 11L, "a" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 10L)
    val distance = Distance.categoricalDistance(sample1, sample2, correctForLowNumberOfSamples = true)
    assert(distance == 0.2857142857142857)
  }

  "Categorial distance should compute correct linf_robust with different bin value" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 10L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 5L)
    val sample2 = scala.collection.mutable.Map(
      "f" -> 11L, "a" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 10L)
    val distance = Distance.categoricalDistance(sample1, sample2)
    assert(distance == 0.0)
  }

  "Categorial distance should compute correct linf_robust with different alpha value .003" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 207L, "b" -> 20L, "c" -> 25L, "d" -> 14L, "e" -> 25L, "g" -> 13L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 22L, "b" -> 20L, "c" -> 25L, "d" -> 12L, "e" -> 13L, "f" -> 15L)
    val distance = Distance.categoricalDistance(sample1, sample2, method = LInfinityMethod(alpha = Some(0.003)))
    assert(distance == 0.2726338046550349)
  }

  "Categorial distance should compute correct linf_robust with different alpha value .1" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 207L, "b" -> 20L, "c" -> 25L, "d" -> 14L, "e" -> 25L, "g" -> 13L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 22L, "b" -> 20L, "c" -> 25L, "d" -> 12L, "e" -> 13L, "f" -> 15L)
    val distance = Distance.categoricalDistance(sample1, sample2, method = LInfinityMethod(alpha = Some(0.1)))
    assert(distance == 0.33774199396969184)
  }

  // Tests using chi-square method for categorical variables
  "Categorical distance should compute correct chisquare stats with missing bin values" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 207L, "b" -> 20L, "c" -> 25L, "d" -> 14L, "e" -> 25L, "g" -> 13L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 223L, "b" -> 20L, "c" -> 25L, "d" -> 12L, "e" -> 13L, "f" -> 15L)
    val distance = Distance.categoricalDistance(
      sample1, sample2, correctForLowNumberOfSamples = true, method = ChisquareMethod())
    assert(distance == 28.175042782458068)
  }

  "Categorical distance should compute correct chisquare test with missing bin values" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 207L, "b" -> 20L, "c" -> 25L, "d" -> 14L, "e" -> 25L, "g" -> 13L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 223L, "b" -> 20L, "c" -> 25L, "d" -> 12L, "e" -> 13L, "f" -> 15L)
    val distance = Distance.categoricalDistance(sample1, sample2, method = ChisquareMethod())
    assert(distance ==  3.3640191298478506E-5)
  }

  "Categorical distance should compute correct chisquare test" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 207L, "b" -> 20L, "c" -> 25L, "d" -> 14L, "e" -> 25L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 223L, "b" -> 20L, "c" -> 25L, "d" -> 12L, "e" -> 13L)
    val distance = Distance.categoricalDistance(sample1, sample2, method = ChisquareMethod())
    assert(distance == 0.013227994814265176)
  }

  "Categorical distance should compute correct chisquare distance (low samples) " +
    "with regrouping 2 categories (yates) after normalizing" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 5L, "f" -> 2L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 22L, "c" -> 25L, "d" -> 5L, "e" -> 13L, "f" -> 2L)
    val distance = Distance.categoricalDistance(
      sample1, sample2, correctForLowNumberOfSamples = true, method = ChisquareMethod())
    assert(distance == 8.789790456457125)
  }

  "Categorical distance should compute correct chisquare distance (low samples) with regrouping (yates)" in {
    val baseline = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 40L, "c" -> 30L, "e" -> 4L)
    val sample = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 40L, "c" -> 30L, "d" -> 10L)
    val distance = Distance.categoricalDistance(
      sample, baseline, correctForLowNumberOfSamples = true, method = ChisquareMethod())
    assert(distance == 0.38754325259515626)
  }

  "Categorical distance should compute correct chisquare distance (low samples) " +
    "with regrouping 2 categories (yates)" in {
    val baseline = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 4L, "c" -> 3L, "d" -> 34L)
    val sample = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 4L, "c" -> 3L, "d" -> 27L)
    val distance = Distance.categoricalDistance(
      sample, baseline, correctForLowNumberOfSamples = true, method = ChisquareMethod())
    assert(distance == 1.1507901668129925)
  }

  "Categorical distance should compute correct chisquare distance (low samples) " +
    "with regrouping " +
    "(sum of 2 grouped categories is below threshold, but small categories represent less than 20%) (yates)" in {
    val baseline = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 2L, "c" -> 1L, "d" -> 34L, "e" -> 20L, "f" -> 20L, "g" -> 20L, "h" -> 20L)
    val sample = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 4L, "c" -> 3L, "d" -> 27L, "e" -> 20L, "f" -> 20L, "g" -> 20L, "h" -> 20L)
    val distance = Distance.categoricalDistance(
      sample, baseline, correctForLowNumberOfSamples = true, method = ChisquareMethod())
    assert(distance == 6.827423492761593)
  }

  "Categorical distance should compute correct chisquare distance (low samples) " +
    "with regrouping ( dimensions after regrouping are too small)" in {
    val baseline = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 4L, "c" -> 3L)
    val sample = scala.collection.mutable.Map(
      "a" -> 100L, "b" -> 4L, "c" -> 3L)
    val distance = Distance.categoricalDistance(
      sample, baseline, correctForLowNumberOfSamples = true, method = ChisquareMethod())
    assert(distance.isNaN)
  }

  "Population Stability Index (PSI) test with deciles " in {

    val expected: List[BucketValue] = List(BucketValue(1.0, 1.05, 428), BucketValue(1.05, 1.1, 425), BucketValue(1.1, 1.15, 414),
      BucketValue(1.15, 1.2, 427), BucketValue(1.2, 1.25, 440), BucketValue(1.25, 1.3, 447),
      BucketValue(1.3, 1.35, 380), BucketValue(1.35, 1.4, 386), BucketValue(1.4, 1.45, 444),
      BucketValue(1.45, 1.5, 386))

    val actual: List[BucketValue] = List(BucketValue(1.0, 1.05, 426), BucketValue(1.05, 1.1, 437), BucketValue(1.1, 1.15, 429),
      BucketValue(1.15, 1.2, 391), BucketValue(1.2, 1.25, 469), BucketValue(1.25, 1.3, 433),
      BucketValue(1.3, 1.35, 360), BucketValue(1.35, 1.4, 443), BucketValue(1.4, 1.45, 371),
      BucketValue(1.45, 1.5, 418))

    val distance = Distance.populationStabilityIndex(actual, expected)
    assert(distance == 0.007406694184014186)

  }
}
