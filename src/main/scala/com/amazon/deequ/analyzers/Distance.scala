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

    /** Calculate distance of categorical profiles based on L-Infinity Distance */
    def categoricalDistance(
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

