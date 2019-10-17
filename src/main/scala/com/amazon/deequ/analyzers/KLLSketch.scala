package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.runners.{IllegalAnalyzerParameterException, MetricCalculationException}
import com.amazon.deequ.metrics.{BucketDistribution, BucketValue, KLLMetric}

import scala.collection.mutable.ListBuffer

//import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType,LongType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Try}

case class KLLSketch(
                      column: String,
                      sketchSize: Int,
                      shrinkingFactor: Double = 0.64,
                      maxDetailBins: Integer = KLLSketch.MaximumAllowedDetailBins)
  extends Analyzer[KLLState, KLLMetric] {

  private[this] val PARAM_CHECK: StructType => Unit = { _ =>
    if (maxDetailBins > KLLSketch.MaximumAllowedDetailBins) {
      throw new IllegalAnalyzerParameterException(s"Cannot return KLL Sketch related values for more " +
        s"than ${KLLSketch.MaximumAllowedDetailBins} values")
    }
  }

  val qSketch = new QuantileNonSample[Long](sketchSize,shrinkingFactor)

  override def computeStateFrom(data: DataFrame): Option[KLLState] = {

    val streams = data.select(col(column).cast(LongType)).rdd.map(r => r.getLong(0)).collect.toList

    val globalMax = streams.max

    val globalMin = streams.min

    streams.foreach { i =>
      qSketch.update(i)
    }

    Some(KLLState(qSketch, globalMax, globalMin))
  }

  override def computeMetricFrom(state: Option[KLLState]): KLLMetric = {

    state match {

      case Some(theState) =>
        val value: Try[BucketDistribution] = Try {

          val finalSketch = theState.qSketch
          val start = theState.globalMin
          val end = theState.globalMax
          var bucketsList = new ListBuffer[BucketValue]()
//          val PREFIX = "BUCKET_"
          for (i <- 0 until maxDetailBins) {
            val lowBound = start + (end - start) * i / maxDetailBins
            val highBound = start + (end - start) * (i + 1) / maxDetailBins
            if (i == maxDetailBins - 1) {
              bucketsList +=
                BucketValue(lowBound,highBound,finalSketch.getRank(highBound) -
                finalSketch.getRankExclusive(lowBound))
            } else {
              bucketsList += BucketValue(lowBound,highBound,finalSketch.getRankExclusive(highBound) -
                  finalSketch.getRankExclusive(lowBound))
            }
          }

          val parameters = List[Double](finalSketch.getShrinkingFactor, finalSketch.getSketchSize)
//          val sketchMap = Map("parameters" -> Map("c" -> finalSketch.getShrinkingFactor,
//            "k" -> finalSketch.getSketchSize),
//          "data" -> finalSketch.getCompactorItems)
          val data = finalSketch.getCompactorItems

          BucketDistribution(bucketsList.toList, parameters, data)
        }

        KLLMetric(column, value)

      case None =>
        KLLMetric(column, Failure(Analyzers.emptyStateException(this)))
    }
  }

  override def toFailureMetric(exception: Exception): KLLMetric = {
    KLLMetric(column, Failure(MetricCalculationException.wrapIfNecessary(exception)))
  }

  override def preconditions: Seq[StructType => Unit] = {
    PARAM_CHECK :: Preconditions.hasColumn(column) :: Preconditions.isNumeric(column) :: Nil
  }
}

object KLLSketch {
  val NullFieldReplacement = "NullValue"
  val MaximumAllowedDetailBins = 1000
}

