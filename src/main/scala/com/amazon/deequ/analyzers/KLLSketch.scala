package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.catalyst.KLLSketchSerializer
import com.amazon.deequ.analyzers.runners.IllegalAnalyzerParameterException
import com.amazon.deequ.metrics.{BucketDistribution, BucketValue, KLLMetric}
import scala.collection.mutable.ListBuffer
import scala.util.Try
import java.nio.ByteBuffer
import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import com.amazon.deequ.analyzers.runners.MetricCalculationException
import org.apache.spark.sql.DeequFunctions.stateful_kll
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Row}

import scala.util.Failure

/** State for KLL Sketches */
case class KLLState(qSketch: QuantileNonSample[Double], globalMax: Double, globalMin: Double)
  extends State[KLLState] {

  /** Add up states by merging sketches */
  override def sum(other: KLLState): KLLState = {
    val mergedSketch = qSketch.merge(other.qSketch)
    KLLState(mergedSketch, Math.max(globalMax,other.globalMax), Math.min(globalMin,other.globalMin))
  }
}

object KLLState{
  // from Array[Byte] to original type
  def fromBytes(bytes: Array[Byte]): KLLState = {
    val buffer = ByteBuffer.wrap(bytes)
    val min = buffer.getDouble
    val max = buffer.getDouble
    val store = new Array[Byte](buffer.remaining())
    buffer.get(store)
    val obj = KLLSketchSerializer.serializer.deserialize(store)
    KLLState(obj, max, min)
  }

}

/**
 *
 *
 * @param column Which column to compute this aggregation on.
 */
case class KLLSketch(column: String, where: Option[String] = None, sketchSize: Int = KLLSketch.defaultSketchSize,
                     shrinkingFactor: Double = KLLSketch.defaultShrinkingFactor,
                     maxDetailBins: Int = KLLSketch.MaximumAllowedDetailBins)
  extends ScanShareableAnalyzer[KLLState, KLLMetric] {

  private[this] val PARAM_CHECK: StructType => Unit = { _ =>
        if (maxDetailBins > KLLSketch.MaximumAllowedDetailBins) {
          throw new IllegalAnalyzerParameterException(s"Cannot return KLL Sketch related values for more " +
            s"than ${KLLSketch.MaximumAllowedDetailBins} values")
        }
      }

  override def aggregationFunctions(): Seq[Column] = {
    stateful_kll(conditionalSelection(column, where),sketchSize, shrinkingFactor) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[KLLState] = {
    ifNoNullsIn(result, offset) { _ =>
      KLLState.fromBytes(result.getAs[Array[Byte]](offset))
    }

  }

  override def computeMetricFrom(state: Option[KLLState]): KLLMetric = {
    state match {

      case Some(theState) =>
        val value: Try[BucketDistribution] = Try {

          val finalSketch = theState.qSketch
          val start = theState.globalMin
          val end = theState.globalMax
          var bucketsList = new ListBuffer[BucketValue]()
          for (i <- 0 until maxDetailBins) {
            val lowBound = start + (end - start) * i / maxDetailBins.toDouble
            val highBound = start + (end - start) * (i + 1) / maxDetailBins.toDouble
            if (i == maxDetailBins - 1) {
              bucketsList +=
                BucketValue(lowBound,highBound,finalSketch.getRank(highBound) -
                finalSketch.getRankExclusive(lowBound))
            } else {
              bucketsList += BucketValue(lowBound,highBound,finalSketch.getRankExclusive(highBound) -
                  finalSketch.getRankExclusive(lowBound))
            }
          }

          val parameters = List[Double](finalSketch.getShrinkingFactor, finalSketch.getSketchSize.toDouble)
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


  override def preconditions(): Seq[StructType => Unit] = {
    PARAM_CHECK :: hasColumn(column) :: isNumeric(column) :: Nil
  }
}

object KLLSketch {
  val defaultSketchSize = 2
  val defaultShrinkingFactor = 0.64
  val MaximumAllowedDetailBins = 2
}