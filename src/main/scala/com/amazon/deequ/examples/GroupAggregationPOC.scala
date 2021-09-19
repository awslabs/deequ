/** Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  * http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  */

package com.amazon.deequ.examples

import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import org.apache.spark.sql.{Column, Row}
import scala.collection.mutable
import org.apache.spark.sql.types.{DoubleType, StructType}
import com.amazon.deequ.analyzers.Analyzers._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}
import scala.reflect.runtime.universe.TypeTag

import com.amazon.deequ.examples.ExampleUtils.withSpark

case class MyData(
    productName: String,
    totalNumber: Double,
)


case class MeanState(v: Double, n: Long) {
  def metricValue(): Double = v / n

  def +(other: MeanState): MeanState = {
    MeanState(v + other.v, n + other.n)
  }
}

case class MeanAggregator(col: String)
    extends Aggregator[Row, MeanState, Double]
    with Serializable {
  def zero = MeanState(0, 0)

  def reduce(acc: MeanState, x: Row) = {
    Option(x.getAs[Double](col)) match {
      case None    => acc
      case Some(v) => acc + MeanState(v, 1)
    }
  }

  def merge(acc1: MeanState, acc2: MeanState) = acc1 + acc2

  def finish(acc: MeanState) = acc.metricValue()

  def bufferEncoder: Encoder[MeanState] = ExpressionEncoder()
  def outputEncoder: Encoder[Double] = ExpressionEncoder()
}

case class MaxAggregator(col: String)
    extends Aggregator[Row, Double, Double]
    with Serializable {

  def zero = Double.MinValue
  def reduce(acc: Double, x: Row) =
    Math.max(acc, Option(x.getAs[Double](col)).getOrElse(zero))

  def merge(acc1: Double, acc2: Double) = Math.max(acc1, acc2)
  def finish(acc: Double) = acc

  def bufferEncoder: Encoder[Double] = ExpressionEncoder()
  def outputEncoder: Encoder[Double] = ExpressionEncoder()
}

case class GroupByWrapper[K, U, V](
    groupCol: String,
    aggregator: Aggregator[Row, U, V]
)
(implicit tagK: TypeTag[K], tagU: TypeTag[U], tagV: TypeTag[V])
    extends Aggregator[
      Row,
      mutable.Map[K, U],
      Map[K, V]
    ]
    with Serializable {

  def zero = mutable.Map.empty[K, U]

  def reduce(acc: mutable.Map[K, U], row: Row) = {
    val key = row.getAs[K](groupCol)
    acc.update(key, aggregator.reduce(acc.getOrElse(key, aggregator.zero), row))
    acc
  }

  def merge(
      acc1: mutable.Map[K, U],
      acc2: mutable.Map[K, U]
  ): mutable.Map[K, U] = {
    val (small, big) = if (acc1.size < acc2.size) (acc1, acc2) else (acc2, acc1)

    for ((key, u2) <- small) {
      big.update(
        key,
        big.get(key) match {
          case None     => u2
          case Some(u1) => aggregator.merge(u1, u2)
        }
      )
    }
    big
  }

  def finish(acc: mutable.Map[K, U]): Map[K, V] = acc.map { case (k, u) =>
    (k, aggregator.finish(u))
  }.toMap

  override def bufferEncoder: Encoder[mutable.Map[K, U]] = ExpressionEncoder[mutable.Map[K, U]]
  override def outputEncoder: Encoder[Map[K, V]] = ExpressionEncoder[Map[K, V]]
}

/*
This is a small POC to explore how group aggregation could work without using an explicit groupby.
Here we use the Aggregator API to do column aggregation.
The GroupByWrapper can then take an Aggregator and apply it per group key.
*/
object GroupAggregationPOC extends App {

  withSpark { session =>
    val rows = session.sparkContext.parallelize(
      Seq(
        MyData("thingA", 13.0),
        MyData("thingA", 5),
        MyData("thingB", 0.3),
        MyData("thingC", 2.1),
        MyData("thingD", 1.0),
        MyData("thingC", 7.0),
        MyData("thingC", 20),
        MyData("thingE", 20)
      )
    )

    val rawData = session.createDataFrame(rows)

    rawData.show()

    import session.implicits._

    val aggDf = rawData
      .agg(
        MeanAggregator("totalNumber").toColumn.name("mean"),
        GroupByWrapper[String, Double, Double](
          "productName",
          MaxAggregator("totalNumber")
        ).toColumn
          .name("max_by_group"),
        GroupByWrapper[String, MeanState, Double](
          "productName",
          MeanAggregator("totalNumber")
        ).toColumn
          .name("mean_by_group")
      )

    val row = aggDf
      .take(1)(0)

    println(row.getAs[Map[String, Double]](1))
    println(row.getAs[Map[String, Double]](2))
  }
}
