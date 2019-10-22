package com.amazon.deequ.metrics

import scala.util.{Failure, Success, Try}

case class BucketValue(low_value: Double, high_value: Double, count: Long)

case class BucketDistribution(buckets: List[BucketValue], parameters: List[Double], data: Array[Array[Double]]) {

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
}

case class KLLMetric(column: String, value: Try[BucketDistribution]) extends Metric[BucketDistribution] {
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
            DoubleMetric(entity, s"$name.low", instance, Success(distValue.low_value)) ::
              DoubleMetric(entity, s"$name.high", instance, Success(distValue.high_value)) ::
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
