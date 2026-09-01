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

import com.amazon.deequ.analyzers.QuantileNonSample
import org.scalatest.{FlatSpec, Matchers}

class KLLRepeatedItemsTest extends FlatSpec with Matchers {

  "getRankMap" should "accumulate the weights of repeated data points" in {
    val sketch = new QuantileNonSample[Int](4, 0.64)
    Seq(1, 1, 2).foreach(sketch.update)

    val rankMap = sketch.getRankMap()

    // 1 occurs twice, so its rank (number of items <= 1) is 2, not 1.
    rankMap(1) shouldBe 2L
    // 2 is the maximum, so its rank is the total number of items.
    rankMap(2) shouldBe 3L
  }

  it should "report a total weight equal to the number of updates" in {
    val sketch = new QuantileNonSample[Int](4, 0.64)
    val items = Seq(3, 3, 3, 7, 7, 9)
    items.foreach(sketch.update)

    val rankMap = sketch.getRankMap()
    val (_, totalWeight) = rankMap.last

    totalWeight shouldBe items.size.toLong
  }

  "getCDF" should "not under-count repeated data points" in {
    val sketch = new QuantileNonSample[Int](4, 0.64)
    Seq(1, 1, 2).foreach(sketch.update)

    val cdf = sketch.getCDF().toMap

    // P(X <= 1) = 2/3 because 1 occurs twice out of three items.
    cdf(1) shouldBe (2.0 / 3.0 +- 1e-9)
    // The CDF must reach 1.0 at the maximum item.
    cdf(2) shouldBe (1.0 +- 1e-9)
  }

  it should "stay consistent with quantiles for a stream of repeated integers" in {
    val sketch = new QuantileNonSample[Int](256, 0.64)
    // Heavily repeated integers: each of 0..9 appears 100 times.
    (0 until 1000).foreach(i => sketch.update(i % 10))

    val rankMap = sketch.getRankMap()
    val (_, totalWeight) = rankMap.last

    // No compaction happens at this size, so the total weight must be exact.
    totalWeight shouldBe 1000L

    val cdf = sketch.getCDF().toMap
    // 0..4 carry 500 of the 1000 items, and the sketch reports the rank of a
    // value as the count of items <= it, so allow the small sketch error.
    cdf(4) shouldBe (0.5 +- 0.01)
    cdf(9) shouldBe (1.0 +- 1e-9)
  }
}
