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
import com.amazon.deequ.analyzers.{Distance, QuantileNonSample}
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

class KLLDistanceTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport{

  "KLL distance calculator should compute correct linf_simple" in {
    var sample1 = new QuantileNonSample[Double](4, 0.64)
    var sample2 = new QuantileNonSample[Double](4, 0.64)
    sample1.reconstruct(4, 0.64, Array(Array(1, 2, 3, 4)))
    sample2.reconstruct(4, 0.64, Array(Array(2, 3, 4, 5)))
    assert(Distance.numericalDistance(sample1, sample2, true) == 0.25)
  }

  "KLL distance calculator should compute correct linf_robust" in {
    var sample1 = new QuantileNonSample[Double](4, 0.64)
    var sample2 = new QuantileNonSample[Double](4, 0.64)
    sample1.reconstruct(4, 0.64, Array(Array(1, 2, 3, 4)))
    sample2.reconstruct(4, 0.64, Array(Array(2, 3, 4, 5)))
    assert(Distance.numericalDistance(sample1, sample2) == 0.0)
  }

  "Categorial distance should compute correct linf_simple" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 10L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 5L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 11L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 10L)
    assert(Distance.categoricalDistance(sample1,
      sample2, true) == 0.06015037593984962)
  }

  "Categorial distance should compute correct linf_robust" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 10L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 5L)
    val sample2 = scala.collection.mutable.Map(
      "a" -> 11L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 10L)
    assert(Distance.categoricalDistance(sample1, sample2) == 0.0)
  }

  "Categorial distance should compute correct linf_simple with different bin value" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 10L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 5L)
    val sample2 = scala.collection.mutable.Map(
      "f" -> 11L, "a" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 10L)
    assert(Distance.categoricalDistance(sample1,
      sample2, true) == 0.2857142857142857)
  }

  "Categorial distance should compute correct linf_robust with different bin value" in {
    val sample1 = scala.collection.mutable.Map(
      "a" -> 10L, "b" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 5L)
    val sample2 = scala.collection.mutable.Map(
      "f" -> 11L, "a" -> 20L, "c" -> 25L, "d" -> 10L, "e" -> 10L)
    assert(Distance.categoricalDistance(sample1, sample2) == 0.0)
  }
}
