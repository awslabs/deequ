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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.CentralMomentAgg
import org.apache.spark.sql.types._

/** Adjusted version of org.apache.spark.sql.catalyst.expressions.aggregate.StddevPop */
private[sql] case class StatefulStdDevPop(child: Expression) extends CentralMomentAgg(child) {

  override protected def momentOrder = 2

  override def dataType: DataType = StructType(StructField("n", DoubleType) ::
    StructField("avg", DoubleType) :: StructField("m2", DoubleType) :: Nil)

  override val evaluateExpression: Expression = CreateStruct(n :: avg :: m2 :: Nil)

  override def prettyName: String = "stateful_stddev_pop"
}
