/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 *
 */

package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Failure
import scala.util.Success

class ColumnCountTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {
  "ColumnCount" should {
    "return column count for a dataset" in withSparkSession { session =>
      val data = getDfWithStringColumns(session)
      val colCount = ColumnCount()

      val state = colCount.computeStateFrom(data)
      state.isDefined shouldBe true
      state.get.metricValue() shouldBe 5.0

      val metric = colCount.computeMetricFrom(state)
      metric.fullColumn shouldBe None
      metric.value shouldBe Success(5.0)
    }
  }
}
