package com.amazon.deequ.suggestions.rules.interval

import com.amazon.deequ.suggestions.rules.interval.ConfidenceIntervalStrategy.{ConfidenceInterval, defaultConfidence}

import scala.math.BigDecimal.RoundingMode

/**
 * Implements the Wald Interval method for creating a binomial proportion confidence interval.
 *
 * @see <a
 *      href="http://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Normal_approximation_interval">
 *      Normal approximation interval (Wikipedia)</a>
 */
case class WaldIntervalStrategy() extends ConfidenceIntervalStrategy {
  def calculateTargetConfidenceInterval(pHat: Double, numRecords: Long, confidence: Double = defaultConfidence): ConfidenceInterval = {
    validateInput(pHat, confidence)
    val successRatio = BigDecimal(pHat)
    val marginOfError = BigDecimal(calculateZScore(confidence) * math.sqrt(pHat * (1 - pHat) / numRecords))
    val lowerBound = (successRatio - marginOfError).setScale(2, RoundingMode.DOWN).toDouble
    val upperBound = (successRatio + marginOfError).setScale(2, RoundingMode.UP).toDouble
    ConfidenceInterval(lowerBound, upperBound)
  }
}
