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
 * Using Wilson score method for creating a binomial proportion confidence interval.
 *
 * @see <a
 *      href="http://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Wilson_score_interval">
 *      Wilson score interval (Wikipedia)</a>
 */
case class WilsonScoreIntervalStrategy() extends ConfidenceIntervalStrategy {

  def calculateTargetConfidenceInterval(
    pHat: Double, numRecords: Long,
    confidence: Double = defaultConfidence
  ): ConfidenceInterval = {
    validateInput(pHat, confidence)
    val zScore = calculateZScore(confidence)
    val zSquareOverN = math.pow(zScore, 2) / numRecords
    val factor = 1.0 / (1 + zSquareOverN)
    val adjustedSuccessRatio = pHat + zSquareOverN/2
    val marginOfError = zScore * math.sqrt(pHat * (1 - pHat)/numRecords + zSquareOverN/(4 * numRecords))
    val lowerBound = BigDecimal(factor * (adjustedSuccessRatio - marginOfError)).setScale(2, RoundingMode.DOWN).toDouble
    val upperBound = BigDecimal(factor * (adjustedSuccessRatio + marginOfError)).setScale(2, RoundingMode.UP).toDouble
    ConfidenceInterval(lowerBound, upperBound)
  }
}
