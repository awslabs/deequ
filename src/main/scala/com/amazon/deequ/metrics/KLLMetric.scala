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

import com.amazon.deequ.analyzers.QuantileNonSample

import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks._

case class BucketValue(lowValue: Double, highValue: Double, count: Long)

case class BucketDistribution(
    buckets: List[BucketValue],
    parameters: List[Double],
    data: Array[Array[Double]]) {

  def computePercentiles(): Array[Double] = {

    val sketchSize = parameters(0).toInt
    val shrinkingFactor = parameters(1)

    val quantileNonSample = new QuantileNonSample[Double](sketchSize, shrinkingFactor)
    quantileNonSample.reconstruct(sketchSize, shrinkingFactor, data)

    quantileNonSample.quantiles(100)
  }

  /**
   * Get relevant bucketValue with index of bucket.
   * @param key index of bucket
   * @return The metrics for the bucket
   */
  def apply(key: Int): BucketValue = {
    buckets(key)
  }

  /**
   * Find the index of bucket which contains the most items.
   * @return The index of bucket which contains the most items.
   */
  def argmax: Int = {
    var currentMax = 0L
    var maxBucket = 0
    buckets.foreach { bucket =>
      if (bucket.count > currentMax) {
        currentMax = bucket.count
        maxBucket = buckets.indexOf(bucket)
      }
    }
    maxBucket
  }

  /**
   * Check if it is equal with two BucketDistribution.
   * @param obj object to compare
   * @return true if equal
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case that: BucketDistribution =>
        var check = that.isInstanceOf[BucketDistribution] &&
          this.buckets.equals(that.buckets) &&
          this.parameters.equals(that.parameters) &&
          this.data.length == that.data.length
        breakable {
          for (i <- this.data.indices) {
            if (!this.data(i).sameElements(that.data(i))) {
              check = false
              break
            }
          }
        }
        check
      case _ => false
    }
  }

  // TODO not sure if thats correct...
  override def hashCode(): Int = super.hashCode()
}

case class KLLMetric(column: String, value: Try[BucketDistribution])
  extends Metric[BucketDistribution] {

  val entity: Entity.Value = Entity.Column
  val instance: String = column
  val name = "KLL"

  def flatten(): Seq[DoubleMetric] = {
    value
      .map { distribution =>
        val numberOfBuckets = Seq(DoubleMetric(entity, s"$name.buckets", instance,
          Success(distribution.buckets.length.toDouble)))

        val details = distribution.buckets
          .flatMap { distValue =>
            DoubleMetric(entity, s"$name.low", instance, Success(distValue.lowValue)) ::
              DoubleMetric(entity, s"$name.high", instance, Success(distValue.highValue)) ::
              DoubleMetric(entity, s"$name.count", instance, Success(distValue.count)) :: Nil
          }
        numberOfBuckets ++ details
      }
      .recover {
        case e: Exception => Seq(DoubleMetric(entity, s"$name.buckets", instance, Failure(e)))
      }
      .get
  }

}
