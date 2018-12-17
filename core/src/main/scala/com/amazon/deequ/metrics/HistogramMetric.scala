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

package com.amazon.deequ.metrics

import scala.util.{Failure, Success, Try}

case class DistributionValue(absolute: Long, ratio: Double)

case class Distribution(values: Map[String, DistributionValue], numberOfBins: Long) {

  def apply(key: String): DistributionValue = {
    values(key)
  }

  def argmax: String = {
    val (distributionKey, _) = values.toSeq
      .maxBy { case (_, distributionValue) => distributionValue.absolute }

    distributionKey
  }
}

case class HistogramMetric(column: String, value: Try[Distribution]) extends Metric[Distribution] {
  val entity: Entity.Value = Entity.Column
  val instance: String = column
  val name = "Histogram"

  def flatten(): Seq[DoubleMetric] = {
    value
      .map { distribution =>
        val numberOfBins = Seq(DoubleMetric(entity, s"$name.bins", instance,
          Success(distribution.numberOfBins.toDouble)))

        val details = distribution.values
          .flatMap { case (key, distValue) =>
            DoubleMetric(entity, s"$name.abs.$key", instance, Success(distValue.absolute)) ::
              DoubleMetric(entity, s"$name.ratio.$key", instance, Success(distValue.ratio)) :: Nil
          }
        numberOfBins ++ details
      }
      .recover {
        case e: Exception => Seq(DoubleMetric(entity, s"$name.bins", instance, Failure(e)))
      }
      .get
  }

}
