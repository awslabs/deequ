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

import org.apache.spark.sql.Column

import scala.util.{Failure, Success, Try}

object Entity extends Enumeration {
  val Dataset, Column, Mutlicolumn = Value
}

/** Common trait for all data quality metrics */
trait Metric[T] {
  val entity: Entity.Value
  val instance: String
  val name: String
  val value: Try[T]

  /*
   * Composite metric objects e.g histogram can implement this method to
   * returned flattened view of the internal values in terms of double metrics.
   * @see HistogramMetric for sample
   */
  def flatten(): Seq[DoubleMetric]
}

/**
 * Full-column metrics store the entire column of row-level pass/fail results
 */
trait FullColumn {
  val fullColumn: Option[Column] = None

  def sum(colA: Option[Column], colB: Option[Column]): Option[Column] =
    if (colA.equals(colB)) colA else None
}

/** Common trait for all data quality metrics where the value is double */
case class DoubleMetric(
                         entity: Entity.Value,
                         name: String,
                         instance: String,
                         value: Try[Double],
                         override val fullColumn: Option[Column] = None)
  extends Metric[Double] with FullColumn {

  override def flatten(): Seq[DoubleMetric] = Seq(this)
}

case class KeyedDoubleMetric(
    entity: Entity.Value,
    name: String,
    instance: String,
    value: Try[Map[String, Double]])
  extends Metric[Map[String, Double]] {

  override def flatten(): Seq[DoubleMetric] = {
    if (value.isSuccess) {
      value.get.map { case (key, correspondingValue) =>
        DoubleMetric(entity, s"$name-$key", instance, Success(correspondingValue))
      }
      .toSeq
    } else {
      Seq(DoubleMetric(entity, s"$name", instance, Failure(value.failed.get)))
    }
  }
}
