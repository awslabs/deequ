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

package com.amazon.deequ.profiles

import com.amazon.deequ.metrics.Distribution
import com.amazon.deequ.statistics.DataTypeInstances

/* Profiling results for the columns which will be given to the constraint suggestion engine */
abstract class ColumnProfile {
  def column: String
  def completeness: Double
  def approximateNumDistinctValues: Long
  def dataType: DataTypeInstances.Value
  def isDataTypeInferred: Boolean
  def typeCounts: Map[String, Long]
  def histogram: Option[Distribution]
}

case class StandardColumnProfile(
    column: String,
    completeness: Double,
    approximateNumDistinctValues: Long,
    dataType: DataTypeInstances.Value,
    isDataTypeInferred: Boolean,
    typeCounts: Map[String, Long],
    histogram: Option[Distribution])
  extends ColumnProfile

case class NumericColumnProfile(
    column: String,
    completeness: Double,
    approximateNumDistinctValues: Long,
    dataType: DataTypeInstances.Value,
    isDataTypeInferred: Boolean,
    typeCounts: Map[String, Long],
    histogram: Option[Distribution],
    mean: Option[Double],
    maximum: Option[Double],
    minimum: Option[Double],
    sum: Option[Double],
    stdDev: Option[Double],
    approxPercentiles: Option[Seq[Double]])
  extends ColumnProfile

case class ColumnProfiles(
    profiles: Map[String, ColumnProfile],
    numRecords: Long)


object ColumnProfiles {
  val DEFAULT_CARDINALITY_THRESHOLD = 120
}
