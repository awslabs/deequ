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

package com.amazon.deequ.analyzers

import java.nio.ByteBuffer

import scala.collection.mutable.ListBuffer
import scala.util.{Try, Failure}

import com.amazon.deequ.analyzers.catalyst.KLLSketchSerializer
import com.amazon.deequ.analyzers.runners.IllegalAnalyzerParameterException
import com.amazon.deequ.metrics.{BucketDistribution, BucketValue, KLLMetric}
import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import com.amazon.deequ.analyzers.runners.MetricCalculationException

import org.apache.spark.sql.DeequFunctions.stateful_kll
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.col

/**
 * State definition for KLL Sketches.
 * @param qSketch input KLL Sketch object
 * @param globalMax global maximum of the samples represented in KLL Sketch Object
 * @param globalMin global minimum of the samples represented in KLL Sketch Object
 */
case class KLLState(
    qSketch: QuantileNonSample[Double],
    globalMax: Double,
    globalMin: Double)
  extends State[KLLState] {

  /** Add up states by merging sketches */
  override def sum(other: KLLState): KLLState = {
    val mergedSketch = qSketch.merge(other.qSketch)
    KLLState(mergedSketch,
      Math.max(globalMax, other.globalMax),
      Math.min(globalMin, other.globalMin))
  }
}

object KLLState{

  /**
   * Reconstruct the state from the serialized byte arrays.
   * @param bytes the serialized byte arrays of the state
   * @return the state for KLL Sketches
   */
  def fromBytes(bytes: Array[Byte]): KLLState = {
    val buffer = ByteBuffer.wrap(bytes)
    val min = buffer.getDouble
    val max = buffer.getDouble
    val kllBuffer = new Array[Byte](buffer.remaining())
    buffer.get(kllBuffer)
    val kllSketch = KLLSketchSerializer.serializer.deserialize(kllBuffer)
    KLLState(kllSketch, max, min)
  }

}

/**
 * Parameter definition for KLL Sketches.
 * @param sketchSize size of kll sketch
 * @param shrinkingFactor  shrinking factor of kll sketch
 * @param numberOfBuckets  number of buckets
 */
case class KLLParameters(sketchSize: Int, shrinkingFactor: Double, numberOfBuckets: Int)

/**
 * The KLL Sketch analyzer.
 * @param column the column to run the analyzer
 * //@param where constraint expression on the column
 * @param kllParameters parameters of KLL Sketch
 */
case class KLLSketch(
    column: String,
//    where: Option[String] = None,
    kllParameters: Option[KLLParameters] = None)
  extends ScanShareableAnalyzer[KLLState, KLLMetric] {

  var sketchSize: Int = KLLSketch.DEFAULT_SKETCH_SIZE
  var shrinkingFactor: Double = KLLSketch.DEFAULT_SHRINKING_FACTOR
  var numberOfBuckets: Int = KLLSketch.MAXIMUM_ALLOWED_DETAIL_BINS
  if (kllParameters.isDefined) {
    sketchSize = kllParameters.get.sketchSize
    shrinkingFactor = kllParameters.get.shrinkingFactor
    numberOfBuckets = kllParameters.get.numberOfBuckets
  }

  private[this] val PARAM_CHECK: StructType => Unit = { _ =>
    if (numberOfBuckets > KLLSketch.MAXIMUM_ALLOWED_DETAIL_BINS) {
      throw new IllegalAnalyzerParameterException(
        s"Cannot return KLL Sketch related values for more " +
        s"than ${KLLSketch.MAXIMUM_ALLOWED_DETAIL_BINS} values")
    }
  }

  override def aggregationFunctions(): Seq[Column] = {
    // stateful_kll(conditionalSelection(column, where), sketchSize, shrinkingFactor) :: Nil
    stateful_kll(col(column), sketchSize, shrinkingFactor) :: Nil
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
          for (i <- 0 until numberOfBuckets) {
            val lowBound = start + (end - start) * i / numberOfBuckets.toDouble
            val highBound = start + (end - start) * (i + 1) / numberOfBuckets.toDouble
            if (i == numberOfBuckets - 1) {
              bucketsList += BucketValue(lowBound, highBound,
                finalSketch.getRank(highBound) - finalSketch.getRankExclusive(lowBound))
            } else {
              bucketsList += BucketValue(lowBound, highBound,
                finalSketch.getRankExclusive(highBound) - finalSketch.getRankExclusive(lowBound))
            }
          }

          val parameters = List[Double](finalSketch.shrinkingFactor,
            finalSketch.sketchSize.toDouble)
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
  val DEFAULT_SKETCH_SIZE = 2048
  val DEFAULT_SHRINKING_FACTOR = 0.64
  val MAXIMUM_ALLOWED_DETAIL_BINS = 100
}
