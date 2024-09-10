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

import com.amazon.deequ.suggestions.rules.interval.ConfidenceIntervalStrategy.ConfidenceInterval
import com.amazon.deequ.suggestions.rules.interval.ConfidenceIntervalStrategy.defaultConfidence

import scala.math.BigDecimal.RoundingMode

/**
 * Implements the Wald Interval method for creating a binomial proportion confidence interval. Provided for backwards
 * compatibility. using [[WaldIntervalStrategy]] for calculating confidence interval can be problematic when dealing
 *  with small sample sizes or proportions close to 0 or 1. It also have poorer coverage and might produce confidence
 *  limit outside the range of [0,1]
 * @see <a
 *      href="http://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Normal_approximation_interval">
 *      Normal approximation interval (Wikipedia)</a>
 */
@deprecated("WilsonScoreIntervalStrategy is recommended for calculating confidence interval")
case class WaldIntervalStrategy() extends ConfidenceIntervalStrategy {
  def calculateTargetConfidenceInterval(
    pHat: Double,
    numRecords: Long,
    confidence: Double = defaultConfidence
  ): ConfidenceInterval = {
    validateInput(pHat, confidence)
    val successRatio = BigDecimal(pHat)
    val marginOfError = BigDecimal(calculateZScore(confidence) * math.sqrt(pHat * (1 - pHat) / numRecords))
    val lowerBound = (successRatio - marginOfError).setScale(2, RoundingMode.DOWN).toDouble
    val upperBound = (successRatio + marginOfError).setScale(2, RoundingMode.UP).toDouble
    ConfidenceInterval(lowerBound, upperBound)
  }
}
