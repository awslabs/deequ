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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.aggregate.Corr
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/** Adjusted version of org.apache.spark.sql.catalyst.expressions.aggregate.Corr */
private[sql] class StatefulCorrelation(x: Expression, y: Expression) extends Corr(x, y) {

  override def dataType: org.apache.spark.sql.types.DataType =
    StructType(StructField("n", DoubleType) :: StructField("xAvg", DoubleType) ::
      StructField("yAvg", DoubleType) :: StructField("ck", DoubleType) ::
      StructField("xMk", DoubleType) :: StructField("yMk", DoubleType) :: Nil)

  override val evaluateExpression: Expression = {
    CreateStruct(n :: xAvg :: yAvg :: ck :: xMk :: yMk :: Nil)
  }

  override def prettyName: String = "stateful_corr"

  override def canEqual(other: Any): Boolean = other.isInstanceOf[StatefulCorrelation]

  override def equals(other: Any): Boolean = other match {
    case that: StatefulCorrelation =>
      (that canEqual this) && evaluateExpression == that.evaluateExpression
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), evaluateExpression)
    state.map { _.hashCode() }.foldLeft(0) {(a, b) => 31 * a + b }
  }
}
