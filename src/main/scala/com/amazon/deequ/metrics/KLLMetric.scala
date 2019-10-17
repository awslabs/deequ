package com.amazon.deequ.metrics

import scala.util.{Failure, Success, Try}

case class BucketValue(low_value: Long, high_value: Long, count: Long)

case class BucketDistribution(buckets: List[BucketValue], parameters: List[Double], data: Array[Array[Long]]) {

  // Get relevant bucketValue with index of bucket
  def apply(key: Int): BucketValue = {
    buckets(key)
  }

  // find the index of bucket which has the largest count
  def argmax: Int = {
    var max_store = 0L
    var ret = 0
    buckets.foreach { bucket =>
      if (bucket.count > max_store) {
        max_store = bucket.count
        ret = buckets.indexOf(bucket)
      }
    }
    ret
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
