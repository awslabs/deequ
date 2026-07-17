/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.apache.spark.sql

import java.nio.ByteBuffer

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.analyzers.catalyst.KLLSketchSerializer
import com.amazon.deequ.analyzers.{DataTypeHistogram, KLLState, QuantileNonSample}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StatefulAggregatorsTest extends AnyWordSpec with Matchers with SparkContextSpec {

  "StatefulDataType" should {
    "aggregate typed buffers and preserve the serialized state format" in {
      val aggregator = new StatefulDataType()
      val left = aggregator.zero
      val right = aggregator.zero

      Seq(null, "1", "2.5", "true", "text", "1e3")
        .foreach(aggregator.reduce(left, _))
      Seq(null, "-2", "false")
        .foreach(aggregator.reduce(right, _))

      val histogram = DataTypeHistogram.fromBytes(
        aggregator.finish(aggregator.merge(left, right)))

      histogram shouldBe DataTypeHistogram(
        numNull = 2L,
        numFractional = 2L,
        numIntegral = 2L,
        numBoolean = 2L,
        numString = 1L)
    }
  }

  "StatefulKLLSketch" should {
    "aggregate in memory and preserve the serialized state format" in {
      val sketchSize = 2048
      val shrinkingFactor = 0.64
      val aggregator = new StatefulKLLSketch(sketchSize, shrinkingFactor)
      val left = aggregator.zero
      val right = aggregator.zero

      Seq[java.lang.Double](1.0, null, 3.0).foreach(aggregator.reduce(left, _))
      Seq[java.lang.Double](2.0, 4.0).foreach(aggregator.reduce(right, _))

      val bytes = aggregator.finish(aggregator.merge(left, right))
      val state = KLLState.fromBytes(bytes)

      state.globalMin shouldBe 1.0
      state.globalMax shouldBe 4.0
      state.qSketch.getRank(4.0) shouldBe 4L

      val expectedSketch = new QuantileNonSample[Double](sketchSize, shrinkingFactor)
      Seq(1.0, 3.0, 2.0, 4.0).foreach(expectedSketch.update)
      val serializedSketch = KLLSketchSerializer.serializer.serialize(expectedSketch)
      val expectedBytes = ByteBuffer.allocate(2 * java.lang.Double.BYTES + serializedSketch.length)
        .putDouble(1.0)
        .putDouble(4.0)
        .put(serializedSketch)
        .array()

      bytes should contain theSameElementsInOrderAs expectedBytes
    }
  }

  "DeequFunctions" should {
    "execute the typed aggregators across shuffled partitions" in withSparkSession { session =>
      import session.implicits._

      val dataTypeBytes = Seq("1", "2.5", "true", "text", null)
        .toDF("value")
        .repartition(3)
        .agg(DeequFunctions.stateful_datatype(functions.col("value")))
        .head()
        .getAs[Array[Byte]](0)

      DataTypeHistogram.fromBytes(dataTypeBytes) shouldBe
        DataTypeHistogram(1L, 1L, 1L, 1L, 1L)

      val kllBytes = Seq[java.lang.Double](1.0, 2.0, null, 3.0, 4.0)
        .toDF("value")
        .repartition(3)
        .agg(DeequFunctions.stateful_kll(functions.col("value"), 2048, 0.64))
        .head()
        .getAs[Array[Byte]](0)

      val kllState = KLLState.fromBytes(kllBytes)
      kllState.globalMin shouldBe 1.0
      kllState.globalMax shouldBe 4.0
      kllState.qSketch.getRank(4.0) shouldBe 4L
    }

    "produce the same bytes as the legacy UDAFs" in withSparkSession { session =>
      import session.implicits._

      val dataTypeValues = Seq(
        null, "", "0", "-2", "+ 3", "2.5", "-1E-4", "true", "TRUE", "text")
      val dataTypeResults = dataTypeValues
        .toDF("value")
        .repartition(4)
        .agg(
          new LegacyStatefulDataType()(functions.col("value")).as("legacy"),
          DeequFunctions.stateful_datatype(functions.col("value")).as("typed"))
        .head()

      dataTypeResults.getAs[Array[Byte]]("typed") should contain theSameElementsInOrderAs
        dataTypeResults.getAs[Array[Byte]]("legacy")

      val kllValues: Seq[java.lang.Double] =
        (1 to 250).map(value => java.lang.Double.valueOf(value.toDouble)) ++
        Seq[java.lang.Double](
          null,
          -1.0e20,
          1.0e20,
          Double.NaN,
          Double.PositiveInfinity,
          Double.NegativeInfinity)
      val kllResults = kllValues
        .toDF("value")
        .repartition(5)
        .agg(
          new LegacyStatefulKLLSketch(8, 0.64)(functions.col("value")).as("legacy"),
          DeequFunctions.stateful_kll(functions.col("value"), 8, 0.64).as("typed"))
        .head()

      kllResults.getAs[Array[Byte]]("typed") should contain theSameElementsInOrderAs
        kllResults.getAs[Array[Byte]]("legacy")

      val allNullKllResults = Seq[java.lang.Double](null, null)
        .toDF("value")
        .repartition(2)
        .agg(
          new LegacyStatefulKLLSketch(8, 0.64)(functions.col("value")).as("legacy"),
          DeequFunctions.stateful_kll(functions.col("value"), 8, 0.64).as("typed"))
        .head()

      allNullKllResults.getAs[Array[Byte]]("typed") should contain theSameElementsInOrderAs
        allNullKllResults.getAs[Array[Byte]]("legacy")
    }

    "produce the same bytes in standalone physical plans" in withSparkSession { session =>
      import session.implicits._

      val dataTypeData = Seq(
        null, "", "0", "-2", "+ 3", "2.5", "-1E-4", "true", "TRUE", "text")
        .toDF("value")
        .repartition(4)
        .cache()

      val kllData = ((1 to 250).map(value => java.lang.Double.valueOf(value.toDouble)) ++
        Seq[java.lang.Double](
          null,
          -1.0e20,
          1.0e20,
          Double.NaN,
          Double.PositiveInfinity,
          Double.NegativeInfinity))
        .toDF("value")
        .repartition(5)
        .cache()

      try {
        dataTypeData.count()
        kllData.count()

        val legacyDataType = dataTypeData
          .agg(new LegacyStatefulDataType()(functions.col("value")))
          .head()
          .getAs[Array[Byte]](0)
        val typedDataType = dataTypeData
          .agg(DeequFunctions.stateful_datatype(functions.col("value")))
          .head()
          .getAs[Array[Byte]](0)

        typedDataType should contain theSameElementsInOrderAs legacyDataType

        val legacyKll = kllData
          .agg(new LegacyStatefulKLLSketch(8, 0.64)(functions.col("value")))
          .head()
          .getAs[Array[Byte]](0)
        val typedKll = kllData
          .agg(DeequFunctions.stateful_kll(functions.col("value"), 8, 0.64))
          .head()
          .getAs[Array[Byte]](0)

        typedKll should contain theSameElementsInOrderAs legacyKll
      } finally {
        dataTypeData.unpersist()
        kllData.unpersist()
      }
    }

    "preserve empty-input state bytes" in withSparkSession { session =>
      import session.implicits._

      val emptyDataTypeResults = session
        .createDataset(Seq.empty[String])
        .toDF("value")
        .agg(
          new LegacyStatefulDataType()(functions.col("value")).as("legacy"),
          DeequFunctions.stateful_datatype(functions.col("value")).as("typed"))
        .head()

      emptyDataTypeResults.getAs[Array[Byte]]("typed") should contain theSameElementsInOrderAs
        emptyDataTypeResults.getAs[Array[Byte]]("legacy")

      val emptyKllResults = session
        .createDataset(Seq.empty[java.lang.Double])
        .toDF("value")
        .agg(
          new LegacyStatefulKLLSketch(8, 0.64)(functions.col("value")).as("legacy"),
          DeequFunctions.stateful_kll(functions.col("value"), 8, 0.64).as("typed"))
        .head()

      emptyKllResults.getAs[Array[Byte]]("typed") should contain theSameElementsInOrderAs
        emptyKllResults.getAs[Array[Byte]]("legacy")
    }

    "preserve grouped results through object-hash fallback" in withSparkSession { session =>
      import session.implicits._

      val fallbackThreshold = "spark.sql.objectHashAggregate.sortBased.fallbackThreshold"
      val previousThreshold = session.conf.getOption(fallbackThreshold)
      session.conf.set(fallbackThreshold, "1")

      try {
        val input = (1 to 400)
          .map(value => (value % 4, java.lang.Double.valueOf(value.toDouble)))
          .toDF("group", "value")
          .repartition(4)
          .cache()

        try {
          input.count()

          val legacy = input
            .groupBy("group")
            .agg(new LegacyStatefulKLLSketch(8, 0.64)(functions.col("value")).as("state"))
            .collect()
            .map(row => row.getInt(0) -> KLLState.fromBytes(row.getAs[Array[Byte]](1)))
            .toMap
          val typed = input
            .groupBy("group")
            .agg(DeequFunctions.stateful_kll(functions.col("value"), 8, 0.64).as("state"))
            .collect()
            .map(row => row.getInt(0) -> KLLState.fromBytes(row.getAs[Array[Byte]](1)))
            .toMap

          typed.keySet shouldBe legacy.keySet
          typed.foreach { case (group, state) =>
            state.globalMin shouldBe legacy(group).globalMin
            state.globalMax shouldBe legacy(group).globalMax
            state.qSketch.getRank(Double.PositiveInfinity) shouldBe
              legacy(group).qSketch.getRank(Double.PositiveInfinity)
          }
        } finally {
          input.unpersist()
        }
      } finally {
        previousThreshold match {
          case Some(value) => session.conf.set(fallbackThreshold, value)
          case None => session.conf.unset(fallbackThreshold)
        }
      }
    }
  }
}
