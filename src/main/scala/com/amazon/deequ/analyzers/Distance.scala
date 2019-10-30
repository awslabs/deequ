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

case class CategoricalBucket(value: String, count: Long)

case class Categorical(buckets: List[CategoricalBucket])

class Distance {

    /** Calculate distance of numerical profiles based on KLL Sketch
     *  This formula is based on  “Two-sample Kolmogorov–Smirnov test"
     *  Reference: https://en.m.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test */
    def calculateNumericalDistance(
      sample1: QuantileNonSample[Double],
      sample2: QuantileNonSample[Double],
      isSimple: Boolean = false)
    : Double = {
      val rankMap1 = sample1.getRankMap()
      val rankMap2 = sample2.getRankMap()
      val combinedKeys = rankMap1.keySet.union(rankMap2.keySet)
      val n = rankMap1.valuesIterator.max.toDouble
      val m = rankMap2.valuesIterator.max.toDouble
      var linfSimple = 0.0

      combinedKeys.foreach { key =>
        linfSimple = Math.max(linfSimple,
          Math.abs(sample1.getRank(key, rankMap1) / n - sample2.getRank(key, rankMap2) / m))
      }
      val linfRobust = Math.max(0.0, linfSimple - 1.8 * Math.sqrt((n + m) / (n * m) ))
      if (isSimple) {
        linfSimple
      } else {
        linfRobust
      }
    }

    /** Calculate distance of categorical profiles based on L-Infinity Distance */
    def calculateCategoricalDistance(
      sample1: Categorical,
      sample2: Categorical,
      isSimple: Boolean = false)
    : Double = {

      val buckets1 = sample1.buckets
      val buckets2 = sample2.buckets
      var countMap1 = scala.collection.mutable.Map[String, Long]()
      var countMap2 = scala.collection.mutable.Map[String, Long]()
      var n = 0.0
      var m = 0.0
      buckets1.foreach { bucket =>
        n += bucket.count
        countMap1 += (bucket.value -> bucket.count)
      }
      buckets2.foreach { bucket =>
        m += bucket.count
        countMap2 += (bucket.value -> bucket.count)
      }
      val combinedKeys = countMap1.keySet.union(countMap2.keySet)
      var linfSimple = 0.0
      combinedKeys.foreach { key =>
        linfSimple = Math.max(linfSimple,
          Math.abs(countMap1.getOrElse(key, 0L) / n - countMap2.getOrElse(key, 0L) / m))
      }
      val linfRobust = Math.max(0.0, linfSimple - 1.8 * Math.sqrt((n + m) / (n * m) ))
      if (isSimple) {
        linfSimple
      } else {
        linfRobust
      }
    }
}

object Distance{
  val distance: Distance = new Distance
}
