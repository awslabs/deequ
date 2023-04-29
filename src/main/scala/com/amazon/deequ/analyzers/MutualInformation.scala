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

import com.amazon.deequ.analyzers.Analyzers.COUNT_COL
import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType

/**
  * Mutual Information describes how much information about one column can be inferred from another
  * column.
  *
  * If two columns are independent of each other, then nothing can be inferred from one column about
  * the other, and mutual information is zero. If there is a functional dependency of one column to
  * another and vice versa, then all information of the two columns are shared, and mutual
  * information is the entropy of each column.
  */
case class MutualInformation(columns: Seq[String], where: Option[String] = None)
  extends FrequencyBasedAnalyzer(columns)
    with FilterableAnalyzer {

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): DoubleMetric = {

    state match {

      case Some(theState) =>
        val total = theState.numRows
        val Seq(col1, col2) = columns

        val freqCol1 = s"__deequ_f1_$col1"
        val freqCol2 = s"__deequ_f2_$col2"

        val jointStats = theState.frequencies

        val marginalStats1 = jointStats
          .select(col1, COUNT_COL)
          .groupBy(col1)
          .agg(sum(COUNT_COL).as(freqCol1))

        val marginalStats2 = jointStats
          .select(col2, COUNT_COL)
          .groupBy(col2)
          .agg(sum(COUNT_COL).as(freqCol2))


        val miUdf = udf {
          (px: Double, py: Double, pxy: Double) =>
            (pxy / total) * math.log((pxy / total) / ((px / total) * (py / total)))
        }

        val miCol = s"__deequ_mi_${col1}_$col2"
        val value = jointStats
          .join(marginalStats1, usingColumn = col1)
          .join(marginalStats2, usingColumn = col2)
          .withColumn(miCol, miUdf(col(freqCol1), col(freqCol2), col(COUNT_COL)))
          .agg(sum(miCol))

        val resultRow = value.head()

        if (resultRow.isNullAt(0)) {
          metricFromEmpty(this, "MutualInformation", columns.mkString(","), Entity.Multicolumn)
        } else {
          metricFromValue(resultRow.getDouble(0), "MutualInformation", columns.mkString(","),
            Entity.Multicolumn)
        }

      case None =>
        metricFromEmpty(this, "MutualInformation", columns.mkString(","), Entity.Multicolumn)
    }
  }


  /** We need at least one grouping column, and all specified columns must exist */
  override def preconditions: Seq[StructType => Unit] = {
    Preconditions.exactlyNColumns(columns, 2) +: super.preconditions
  }

  override def toFailureMetric(exception: Exception): DoubleMetric = {
    metricFromFailure(exception, "MutualInformation", columns.mkString(","), Entity.Multicolumn)
  }

  override def filterCondition: Option[String] = where
}

object MutualInformation {
  def apply(columnA: String, columnB: String): MutualInformation = {
    new MutualInformation(columnA :: columnB :: Nil)
  }
}
