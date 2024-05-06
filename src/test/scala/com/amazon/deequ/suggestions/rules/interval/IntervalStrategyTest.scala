package com.amazon.deequ.suggestions.rules.interval

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.suggestions.rules.interval.ConfidenceIntervalStrategy.ConfidenceInterval
import com.amazon.deequ.utils.FixtureSupport
import org.scalamock.scalatest.MockFactory
import org.scalatest.wordspec.AnyWordSpec

class IntervalStrategyTest extends AnyWordSpec with FixtureSupport with SparkContextSpec
  with MockFactory {
  "WaldIntervalStrategy" should {
    "be calculated correctly" in {
      assert(WaldIntervalStrategy().calculateTargetConfidenceInterval(1.0, 20L) == ConfidenceInterval(1.0, 1.0))
      assert(WaldIntervalStrategy().calculateTargetConfidenceInterval(0.5, 100L) == ConfidenceInterval(0.4, 0.6))
      assert(WaldIntervalStrategy().calculateTargetConfidenceInterval(0.4, 100L) == ConfidenceInterval(0.3, 0.5))
      assert(WaldIntervalStrategy().calculateTargetConfidenceInterval(0.6, 100L) == ConfidenceInterval(0.5, 0.7))
      assert(WaldIntervalStrategy().calculateTargetConfidenceInterval(0.90, 100L) == ConfidenceInterval(0.84, 0.96))
      assert(WaldIntervalStrategy().calculateTargetConfidenceInterval(1.0, 100L) == ConfidenceInterval(1.0, 1.0))
    }
  }

  "WilsonIntervalStrategy" should {
    "be calculated correctly" in {
      assert(WilsonScoreIntervalStrategy().calculateTargetConfidenceInterval(1.0, 20L) == ConfidenceInterval(0.83, 1.0))
      assert(WilsonScoreIntervalStrategy().calculateTargetConfidenceInterval(0.5, 100L) == ConfidenceInterval(0.4, 0.6))
      assert(WilsonScoreIntervalStrategy().calculateTargetConfidenceInterval(0.4, 100L) == ConfidenceInterval(0.3, 0.5))
      assert(WilsonScoreIntervalStrategy().calculateTargetConfidenceInterval(0.6, 100L) == ConfidenceInterval(0.5, 0.7))
      assert(WilsonScoreIntervalStrategy().calculateTargetConfidenceInterval(0.90, 100L) == ConfidenceInterval(0.82, 0.95))
      assert(WilsonScoreIntervalStrategy().calculateTargetConfidenceInterval(1.0, 100L) == ConfidenceInterval(0.96, 1.0))
    }
  }
}
