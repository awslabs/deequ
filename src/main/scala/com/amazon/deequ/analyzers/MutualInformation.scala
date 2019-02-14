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

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.apache.spark.sql.functions.{col, sum, udf}
import org.apache.spark.sql.types.StructType
import Analyzers.COUNT_COL
import com.amazon.deequ.schema.ColumnName

/**
  * Mutual Information describes how much information about one column can be inferred from another
  * column.
  *
  * If two columns are independent of each other, then nothing can be inferred from one column about
  * the other, and mutual information is zero. If there is a functional dependency of one column to
  * another and vice versa, then all information of the two columns are shared, and mutual
  * information is the entropy of each column.
  */
case class MutualInformation(columns: Seq[String])
  extends FrequencyBasedAnalyzer(columns) {

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): DoubleMetric = {

    state match {

      case Some(theState) =>
        val total = theState.numRows
        val Seq(unsafeCol1, unsafeCol2) = columns
        val Seq(safeCol1, safeCol2) = columns.map { ColumnName.sanitize }

        val unsafeFreqCol1 = s"__deequ_f1_$unsafeCol1"
        val unsafeFreqCol2 = s"__deequ_f2_$unsafeCol2"

        val jointStats = theState.frequencies

        // NOTE: `.as(...)` will properly escape its input column name alias

        val marginalStats1 = jointStats
          .select(safeCol1, COUNT_COL)
          .groupBy(safeCol1)
          .agg(sum(COUNT_COL).as(unsafeFreqCol1))

        val marginalStats2 = jointStats
          .select(safeCol2, COUNT_COL)
          .groupBy(safeCol2)
          .agg(sum(COUNT_COL).as(unsafeFreqCol2))


        val miUdf = udf {
          (px: Double, py: Double, pxy: Double) =>
            (pxy / total) * math.log((pxy / total) / ((px / total) * (py / total)))
        }

        val unsafeMiCol = s"__deequ_mi_${unsafeCol1}_$unsafeCol2"
        // NOTE: join(..., usingColumn = ...) will properly escape the "usingColumn" value
        val value = jointStats
          .join(marginalStats1, usingColumn = unsafeCol1)
          .join(marginalStats2, usingColumn = unsafeCol2)
          .withColumn(
            unsafeMiCol,
            miUdf(
              col(ColumnName.sanitize(unsafeFreqCol1)),
              col(ColumnName.sanitize(unsafeFreqCol2)),
              col(COUNT_COL)
            )
          )
          .agg(sum(col(ColumnName.sanitize(unsafeMiCol))))

        val resultRow = value.head()

        if (resultRow.isNullAt(0)) {
          metricFromEmpty(this, "MutualInformation", columns.mkString(","), Entity.Mutlicolumn)
        } else {
          metricFromValue(resultRow.getDouble(0), "MutualInformation", columns.mkString(","),
            Entity.Mutlicolumn)
        }

      case None =>
        metricFromEmpty(this, "MutualInformation", columns.mkString(","), Entity.Mutlicolumn)
    }
  }


  /** We need at least one grouping column, and all specified columns must exist */
  override def preconditions: Seq[StructType => Unit] = {
    Preconditions.exactlyNColumns(columns, 2) +: super.preconditions
  }

  override def toFailureMetric(exception: Exception): DoubleMetric = {
    metricFromFailure(exception, "MutualInformation", columns.mkString(","), Entity.Mutlicolumn)
  }
}

object MutualInformation {
  def apply(columnA: String, columnB: String): MutualInformation = {
    new MutualInformation(columnA :: columnB :: Nil)
  }
}
