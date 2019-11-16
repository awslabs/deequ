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

package com.amazon.deequ.analyzers.runners

import com.amazon.deequ.analyzers.{Analyzer, KLLParameters, KLLSketch, KLLState, QuantileNonSample, State, StateLoader, StatePersister}
import com.amazon.deequ.metrics.Metric
import org.apache.spark.sql.types.{ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

@SerialVersionUID(1L)
abstract class UntypedQuantileNonSample(sketchSize: Int, shrinkingFactor: Double)
  extends Serializable  {

  var min: Double = Int.MaxValue.toDouble
  var max: Double = Int.MinValue.toDouble
  var sketch: QuantileNonSample[Double] = new QuantileNonSample[Double](sketchSize, shrinkingFactor)

  def itemAsDouble(item: Any): Double

  def updateUntyped(item: Any): Unit = {
    this.min = math.min(this.min, itemAsDouble(item))
    this.max = math.max(this.max, itemAsDouble(item))
    sketch.update(itemAsDouble(item))
  }

  def mergeUntyped(other: UntypedQuantileNonSample): Unit = {
    this.min = math.min(this.min, other.min)
    this.max = math.max(this.max, other.max)
    this.sketch = this.sketch.merge(other.sketch)
  }

  def asKLLState(): KLLState = {
    KLLState(sketch, max, min)
  }
}

@SerialVersionUID(1L)
class LongQuantileNonSample(sketchSize: Int, shrinkingFactor: Double)
  extends UntypedQuantileNonSample(sketchSize, shrinkingFactor) with Serializable {
  override def itemAsDouble(item: Any): Double = item.asInstanceOf[Long].toDouble
}

@SerialVersionUID(1L)
class IntQuantileNonSample(sketchSize: Int, shrinkingFactor: Double)
  extends UntypedQuantileNonSample(sketchSize, shrinkingFactor) with Serializable {
  override def itemAsDouble(item: Any): Double = item.asInstanceOf[Int].toDouble
}

@SerialVersionUID(1L)
class ShortQuantileNonSample(sketchSize: Int, shrinkingFactor: Double)
  extends UntypedQuantileNonSample(sketchSize, shrinkingFactor) with Serializable {
  override def itemAsDouble(item: Any): Double = item.asInstanceOf[Short].toDouble
}

@SerialVersionUID(1L)
class ByteQuantileNonSample(sketchSize: Int, shrinkingFactor: Double)
  extends UntypedQuantileNonSample(sketchSize, shrinkingFactor) with Serializable {
  override def itemAsDouble(item: Any): Double = item.asInstanceOf[Byte].toDouble
}

@SerialVersionUID(1L)
class DoubleQuantileNonSample(sketchSize: Int, shrinkingFactor: Double)
  extends UntypedQuantileNonSample(sketchSize, shrinkingFactor) with Serializable {
  override def itemAsDouble(item: Any): Double = item.asInstanceOf[Double]
}

@SerialVersionUID(1L)
class FloatQuantileNonSample(sketchSize: Int, shrinkingFactor: Double)
  extends UntypedQuantileNonSample(sketchSize, shrinkingFactor) with Serializable {
  override def itemAsDouble(item: Any): Double = item.asInstanceOf[Float].toDouble
}

object KLLRunner {

  def computeKLLSketchesInExtraPass(
      data: DataFrame,
      analyzers: Seq[Analyzer[State[_], Metric[_]]],
      aggregateWith: Option[StateLoader] = None,
      saveStatesTo: Option[StatePersister] = None)
    : AnalyzerContext = {

    val kllAnalyzers = analyzers.map { _.asInstanceOf[KLLSketch] }

    val columnsAndParameters = kllAnalyzers
      .map { analyzer => (analyzer.column, analyzer.kllParameters) }
        .toMap

    val sketching = sketchPartitions(columnsAndParameters, data.schema)_

    val sketchPerColumn =
      data.rdd
        .mapPartitions(sketching, preservesPartitioning = true)
        .treeReduce { case (columnAndSketchesA, columnAndSketchesB) =>
            columnAndSketchesA.map { case (column, sketch) =>
              sketch.mergeUntyped(columnAndSketchesB(column))
              column -> sketch
            }
        }

    val metricsByAnalyzer = kllAnalyzers.map { analyzer =>
      val kllState = sketchPerColumn(analyzer.column).asKLLState()
      val metric = analyzer.calculateMetric(Some(kllState), aggregateWith, saveStatesTo)

      analyzer -> metric
    }

    AnalyzerContext(metricsByAnalyzer.toMap[Analyzer[_, Metric[_]], Metric[_]])
  }

  private[this] def emptySketches(
      columnsAndParameters: Map[String, Option[KLLParameters]],
      schema: StructType): Map[String, UntypedQuantileNonSample] = {

    columnsAndParameters.map { case (column, parameters) =>

      val (sketchSize, shrinkingFactor) = parameters match {
        case Some(kllParameters) => (kllParameters.sketchSize, kllParameters.shrinkingFactor)
        case _ => (KLLSketch.DEFAULT_SKETCH_SIZE, KLLSketch.DEFAULT_SHRINKING_FACTOR)
      }

      val sketch: UntypedQuantileNonSample = schema(column).dataType match {
        case DoubleType => new DoubleQuantileNonSample(sketchSize, shrinkingFactor)
        case FloatType => new FloatQuantileNonSample(sketchSize, shrinkingFactor)
        case ByteType => new ByteQuantileNonSample(sketchSize, shrinkingFactor)
        case ShortType => new ShortQuantileNonSample(sketchSize, shrinkingFactor)
        case IntegerType => new IntQuantileNonSample(sketchSize, shrinkingFactor)
        case LongType => new LongQuantileNonSample(sketchSize, shrinkingFactor)
        // TODO at the moment, we will throw exceptions for Decimals
        case _ => throw new IllegalArgumentException(s"Cannot handle ${schema(column).dataType}")
      }

      column -> sketch
    }
  }

  private[this] def sketchPartitions(
      columnsAndParameters: Map[String, Option[KLLParameters]],
      schema: StructType)(rows: Iterator[Row])
    : Iterator[Map[String, UntypedQuantileNonSample]] = {

    val columnsAndSketches = emptySketches(columnsAndParameters, schema)

    val namesToIndexes = schema.fields
      .map { _.name }
      .zipWithIndex
      .toMap

    // Include the index to avoid a lookup per row
    val indexesAndSketches = columnsAndSketches.map { case (column, sketch) =>
      (namesToIndexes(column), sketch )
    }

    while (rows.hasNext) {
      val row = rows.next()
      indexesAndSketches.foreach { case (index, sketch) =>
        if (!row.isNullAt(index)) {
          sketch.updateUntyped(row.get(index))
        }
      }
    }

    Iterator.single(columnsAndSketches)
  }

}
