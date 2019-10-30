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

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest._

import scala.util.Random
import com.amazon.deequ.analyzers.QuantileNonSample

class KLLProbTest extends FlatSpec with Matchers {

  "sketch" should "not crash or have a huge error for simple stream" in {

    for (k <- Array(100, 1000, 50000)) {
      val qSketch = new QuantileNonSample[Int](k)
      val streamLen = 1000000
      val epsilon = 100 / k.toDouble

      // test on zoom-in sketch: 1, n, 2, n-1, ...
      (1 to streamLen / 2).foreach { i =>
        qSketch.update(i)
        qSketch.update(streamLen + 1 - i)
      }
      var count = 0
      // select several items to check, save test time
      val step = Math.ceil(Math.max(epsilon * 0.2 * streamLen, 1)).toInt
      var counter = 1

      while (counter < streamLen) {
        val estimate_r = qSketch.getRank(counter)
        val actual_r = counter
        val diff_r = Math.abs(estimate_r - actual_r)
        if (diff_r >= epsilon * streamLen) {
          count += 1
        }
        counter += step
      }
      count == 0 should be(true)
    }
  }

  "sketch" should "not crash or have a huge error for random stream" in {

    for (k <- Array(100, 1000, 50000)) {
      val qSketch = new QuantileNonSample[Int](k)
      val streamLen = 1000000
      val epsilon = 100 / k.toDouble

      // test on random distribution
      val seed = 1
      val randomGenerator = new Random(seed)
      val shuffleList = randomGenerator.shuffle(List.range(1, streamLen))
      shuffleList.foreach(item => qSketch.update(item))
      var count = 0
      // select several items to check, save test time
      val step = Math.ceil(Math.max(epsilon * 0.2 * streamLen, 1)).toInt
      var counter = 1

      while (counter < streamLen) {
        val estimate_r = qSketch.getRank(counter)
        val actual_r = counter
        val diff_r = Math.abs(estimate_r - actual_r)
        if (diff_r >= epsilon * streamLen) {
          count += 1
        }
        counter += step
      }
      count == 0 should be(true)
    }
  }


    "sketch" should "not crash or have a huge error for merged stream" in {

      for (k <- Array(100, 1000, 50000)) {
        var lastMerge = new QuantileNonSample[Int](k)
        val partitionLen = 100000
        val merges = 10
        val epsilon = 100 / k.toDouble

        // test on merged stream
        for (merge <- 0 until merges) {
          var newMerge = new QuantileNonSample[Int](k)
          (1 to partitionLen / 2).foreach { i =>
            newMerge.update(i + merge * partitionLen)
            newMerge.update(merge * partitionLen + partitionLen + 1 - i)
          }
          lastMerge = lastMerge.merge(newMerge)
        }
        var count = 0
        // select several items to check, save test time
        val step = Math.ceil(Math.max(epsilon * 0.2 * partitionLen * merges, 1)).toInt
        var counter = 1

        while (counter < partitionLen * merges) {
          val estimate_r = lastMerge.getRank(counter)
          val actual_r = counter
          val diff_r = Math.abs(estimate_r - actual_r)
          if (diff_r >= epsilon * partitionLen * merges) {
            count += 1
          }
          counter += step
        }
        count == 0 should be(true)
      }
    }
}
