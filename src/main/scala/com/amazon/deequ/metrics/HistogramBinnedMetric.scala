/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.metrics

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class BinData(binStart: Double, binEnd: Double, frequency: Long, ratio: Double)

case class DistributionBinned(
  bins: Vector[BinData],
  numberOfBins: Long
) {

  def apply(index: Int): BinData = bins(index)

  def getInterval(index: Int): String = {
    val bin = bins(index)
    if (index == bins.length - 1) {
      f"[${bin.binStart}%.2f, ${bin.binEnd}%.2f]"
    } else {
      f"[${bin.binStart}%.2f, ${bin.binEnd}%.2f)"
    }
  }
}

case class HistogramBinnedMetric(column: String, value: Try[DistributionBinned]) extends Metric[DistributionBinned] {

  val entity = Entity.Column
  val instance = column
  val name = "HistogramBinned"

  def flatten(): Seq[DoubleMetric] = {
    value
      .map { distribution =>
        val numberOfBins = DoubleMetric(entity, s"$name.bins", instance,
          Success(distribution.numberOfBins))

        val details = distribution.bins.zipWithIndex.map { case (binData, index) =>
          DoubleMetric(entity, s"$name.abs.bin$index", instance, Success(binData.frequency))
        }
        numberOfBins +: details
      }
      .recover {
        case e: Exception => Seq(DoubleMetric(entity, s"$name.bins", instance, Failure(e)))
      }
      .get
  }
}
