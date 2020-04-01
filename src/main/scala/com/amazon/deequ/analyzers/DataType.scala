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

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNotNested}
import com.amazon.deequ.analyzers.runners.MetricCalculationException
import com.amazon.deequ.metrics.{Distribution, DistributionValue, HistogramMetric}
import org.apache.spark.sql.DeequFunctions.stateful_datatype
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Row}

import scala.util.{Failure, Success}

/** Data type instances */
object DataTypeInstances extends Enumeration {
  val Unknown: Value = Value(0)
  val Fractional: Value = Value(1)
  val Integral: Value = Value(2)
  val Boolean: Value = Value(3)
  val String: Value = Value(4)
}

case class DataTypeHistogram(
    numNull: Long,
    numFractional: Long,
    numIntegral: Long,
    numBoolean: Long,
    numString: Long)
  extends State[DataTypeHistogram] {

  override def sum(other: DataTypeHistogram): DataTypeHistogram = {
    DataTypeHistogram(numNull + other.numNull, numFractional + other.numFractional,
      numIntegral + other.numIntegral, numBoolean + other.numBoolean, numString + other.numString)
  }
}

object DataTypeHistogram {

  val SIZE_IN_BYTES = 40
  private[deequ] val NULL_POS = 0
  private[deequ] val FRACTIONAL_POS = 1
  private[deequ] val INTEGRAL_POS = 2
  private[deequ] val BOOLEAN_POS = 3
  private[deequ] val STRING_POS = 4

  def fromBytes(bytes: Array[Byte]): DataTypeHistogram = {
    require(bytes.length == SIZE_IN_BYTES)
    val buffer = ByteBuffer.wrap(bytes).asLongBuffer().asReadOnlyBuffer()
    val numNull = buffer.get(NULL_POS)
    val numFractional = buffer.get(FRACTIONAL_POS)
    val numIntegral = buffer.get(INTEGRAL_POS)
    val numBoolean = buffer.get(BOOLEAN_POS)
    val numString = buffer.get(STRING_POS)

    DataTypeHistogram(numNull, numFractional, numIntegral, numBoolean, numString)
  }

  def toBytes(
      numNull: Long,
      numFractional: Long,
      numIntegral: Long,
      numBoolean: Long,
      numString: Long)
    : Array[Byte] = {

    val out = ByteBuffer.allocate(SIZE_IN_BYTES)
    val outB = out.asLongBuffer()

    outB.put(numNull)
    outB.put(numFractional)
    outB.put(numIntegral)
    outB.put(numBoolean)
    outB.put(numString)

    // TODO avoid allocation
    val bytes = new Array[Byte](out.remaining)
    out.get(bytes)
    bytes
  }

  def toDistribution(hist: DataTypeHistogram): Distribution = {
    val totalObservations =
      hist.numNull + hist.numString + hist.numBoolean + hist.numIntegral + hist.numFractional

    Distribution(Map(
      DataTypeInstances.Unknown.toString ->
        DistributionValue(hist.numNull, hist.numNull.toDouble / totalObservations),
      DataTypeInstances.Fractional.toString ->
        DistributionValue(hist.numFractional, hist.numFractional.toDouble / totalObservations),
      DataTypeInstances.Integral.toString ->
        DistributionValue(hist.numIntegral, hist.numIntegral.toDouble / totalObservations),
      DataTypeInstances.Boolean.toString ->
        DistributionValue(hist.numBoolean, hist.numBoolean.toDouble / totalObservations),
      DataTypeInstances.String.toString ->
        DistributionValue(hist.numString, hist.numString.toDouble / totalObservations)),
      numberOfBins = 5)
  }

  def determineType(dist: Distribution): DataTypeInstances.Value = {

    import DataTypeInstances._

    // If all are unknown, we can't decide
    if (ratioOf(Unknown, dist) == 1.0) {
      Unknown
    } else {
      // If we saw string values or a mix of boolean and numbers, we decide for String
      if (ratioOf(String, dist) > 0.0 ||
        (ratioOf(Boolean, dist) > 0.0 &&
          (ratioOf(Integral, dist) > 0.0 || ratioOf(Fractional, dist) > 0.0))) {
        String
      } else {
        // If we have boolean (but no numbers, because we checked for that), we go with boolean
        if (ratioOf(Boolean, dist) > 0.0) {
          Boolean
        } else {
          // If we have seen one fractional, we go with that type
          if (ratioOf(Fractional, dist) > 0.0) {
            Fractional
          } else {
            Integral
          }
        }
      }
    }
  }

  private[this] def ratioOf(key: DataTypeInstances.Value, distribution: Distribution): Double = {
    distribution.values
      .getOrElse(key.toString, DistributionValue(0L, 0.0))
      .ratio
  }
}

case class DataType(
    column: String,
    where: Option[String] = None)
  extends ScanShareableAnalyzer[DataTypeHistogram, HistogramMetric]
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    stateful_datatype(conditionalSelection(column, where)) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[DataTypeHistogram] = {
    ifNoNullsIn(result, offset) { _ =>
      DataTypeHistogram.fromBytes(result.getAs[Array[Byte]](offset))
    }
  }

  override def computeMetricFrom(state: Option[DataTypeHistogram]): HistogramMetric = {
    state match {
      case Some(histogram) =>
        HistogramMetric(column, Success(DataTypeHistogram.toDistribution(histogram)))
      case _ =>
        toFailureMetric(emptyStateException(this))
    }
  }

  override def toFailureMetric(exception: Exception): HistogramMetric = {
    HistogramMetric(column, Failure(MetricCalculationException.wrapIfNecessary(exception)))
  }

  override def preconditions: Seq[StructType => Unit] = {
    hasColumn(column) +: isNotNested(column) +: super.preconditions
  }

  override def filterCondition: Option[String] = where
}
