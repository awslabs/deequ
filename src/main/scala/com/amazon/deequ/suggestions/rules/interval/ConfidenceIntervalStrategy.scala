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

import breeze.stats.distributions.{Gaussian, Rand}
import com.amazon.deequ.suggestions.rules.interval.ConfidenceIntervalStrategy.{ConfidenceInterval, defaultConfidence}

/**
 * Strategy for calculate confidence interval
 * */
trait ConfidenceIntervalStrategy {

  /**
    * Generated confidence interval interval
    * @param pHat sample of the population that share a trait
    * @param numRecords overall number of records
    * @param confidence confidence level of method used to estimate the interval.
    * @return
    */
  def calculateTargetConfidenceInterval(
    pHat: Double,
    numRecords: Long,
    confidence: Double = defaultConfidence
  ): ConfidenceInterval

  def validateInput(pHat: Double, confidence: Double): Unit = {
    require(0.0 <= pHat && pHat <= 1.0, "pHat must be between 0.0 and 1.0")
    require(0.0 <= confidence && confidence <= 1.0, "confidence must be between 0.0 and 1.0")
  }

  def calculateZScore(confidence: Double): Double = Gaussian(0, 1)(Rand).inverseCdf(1 - ((1.0 - confidence)/ 2.0))
}

object ConfidenceIntervalStrategy {
  val defaultConfidence = 0.95

  case class ConfidenceInterval(lowerBound: Double, upperBound: Double)
}


