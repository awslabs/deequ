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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.PercentileDigest
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.types._

/** Adjusted version of org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
  * (github tag v2.2.0) */
private[sql] case class StatefulApproxQuantile(
    child: Expression,
    accuracyExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[PercentileDigest] with ImplicitCastInputTypes {

  def this(child: Expression, accuracyExpression: Expression) = {
    this(child, accuracyExpression, 0, 0)
  }

  def this(child: Expression) = {
    this(child, Literal(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY))
  }

  // Mark as lazy so that accuracyExpression is not evaluated during tree transformation.
  private lazy val accuracy: Double = accuracyExpression.eval().asInstanceOf[Double]

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(DoubleType, TypeCollection(DoubleType, ArrayType(DoubleType)), IntegerType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!accuracyExpression.foldable) {
      TypeCheckFailure(s"The accuracy provided must be a constant literal")
    } else if (accuracy <= 0) {
      TypeCheckFailure(
        s"The accuracy provided must be a positive integer literal (current value = $accuracy)")
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): PercentileDigest = {
    val relativeError = 1.0D / accuracy
    new PercentileDigest(relativeError)
  }

  override def update(buffer: PercentileDigest, inputRow: InternalRow): PercentileDigest = {
    val value = child.eval(inputRow)
    // Ignore empty rows, for example: percentile_approx(null)
    if (value != null) {
      buffer.add(value.asInstanceOf[Double])
    }
    buffer
  }

  override def merge(buffer: PercentileDigest, other: PercentileDigest): PercentileDigest = {
    buffer.merge(other)
    buffer
  }

  override def eval(buffer: PercentileDigest): Any = {
    // instead of evaluating the PercentileDigest quantile summary here,
    // serialize the digest and return it as byte array
    serialize(buffer)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): StatefulApproxQuantile =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): StatefulApproxQuantile =
    copy(inputAggBufferOffset = newOffset)

  override def children: Seq[Expression] = Seq(child, accuracyExpression)

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def prettyName: String = "percentile_approx"

  override def serialize(digest: PercentileDigest): Array[Byte] = {
    ApproximatePercentile.serializer.serialize(digest)
  }

  override def deserialize(bytes: Array[Byte]): PercentileDigest = {
    ApproximatePercentile.serializer.deserialize(bytes)
  }
}
