/**
 * Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.VerificationResult
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.RowLevelConstraint
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.FullColumn
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompletenessTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Completeness" should {
    "return row-level results for columns" in withSparkSession { session =>

      val data = getDfWithStringColumns(session)

      val completenessCountry = Completeness("Address Line 3")
      val state = completenessCountry.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = completenessCountry.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(_.getAs[Boolean]("new")) shouldBe
        Seq(true, true, true, true, false, true, true, false)
    }
  }
}
