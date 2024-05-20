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

package com.amazon.deequ.suggestions.rules.interval

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.suggestions.rules.interval.ConfidenceIntervalStrategy.ConfidenceInterval
import com.amazon.deequ.utils.FixtureSupport
import org.scalamock.scalatest.MockFactory
import org.scalatest.Inspectors.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.wordspec.AnyWordSpec

class IntervalStrategyTest extends AnyWordSpec with FixtureSupport with SparkContextSpec
  with MockFactory {
  "ConfidenceIntervalStrategy" should {
    "be calculated correctly" in {
      val waldStrategy = WaldIntervalStrategy()
      val wilsonStrategy = WilsonScoreIntervalStrategy()
      val table = Table(
        ("strategy", "pHat", "numRecord", "lowerBound", "upperBound"),
        (waldStrategy, 1.0, 20L, 1.0, 1.0),
        (waldStrategy, 0.5, 100L, 0.4, 0.6),
        (waldStrategy, 0.4, 100L, 0.3, 0.5),
        (waldStrategy, 0.6, 100L, 0.5, 0.7),
        (waldStrategy, 0.9, 100L, 0.84, 0.96),
        (waldStrategy, 1.0, 100L, 1.0, 1.0),
        (wilsonStrategy, 0.01, 20L, 0.00, 0.18),
        (wilsonStrategy, 1.0, 20L, 0.83, 1.0),
        (wilsonStrategy, 0.5, 100L, 0.4, 0.6),
        (wilsonStrategy, 0.4, 100L, 0.3, 0.5),
        (wilsonStrategy, 0.6, 100L, 0.5, 0.7),
        (wilsonStrategy, 0.9, 100L, 0.82, 0.95),
        (wilsonStrategy, 1.0, 100L, 0.96, 1.0),
      )
      forAll(table) { case (strategy, pHat, numRecords, lowerBound, upperBound) =>
        val actualInterval = strategy.calculateTargetConfidenceInterval(pHat, numRecords)
        assert(actualInterval == ConfidenceInterval(lowerBound, upperBound))
      }
    }
  }
}
