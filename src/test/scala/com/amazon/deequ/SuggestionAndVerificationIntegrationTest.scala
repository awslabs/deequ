/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ

import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.Constraint
import com.amazon.deequ.suggestions.ConstraintSuggestionRunner
import com.amazon.deequ.suggestions.Rules
import com.amazon.deequ.suggestions.rules.UniqueIfApproximatelyUniqueRule
import com.amazon.deequ.utilities.ColumnUtil.escapeColumn
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers
import org.scalatest.WordSpec

class SuggestionAndVerificationIntegrationTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "SuggestionAndVerificationIntegration" should {
    "Succeed for all constraints suggested for the data with . in column name" in
      withSparkSession { session =>

        def assertStatusFor(data: DataFrame, checks: Check*)
                           (expectedStatus: CheckStatus.Value)
        : Unit = {
          val verificationSuiteStatus =
            VerificationSuite().onData(data).addChecks(checks).run().status
          assert(verificationSuiteStatus == expectedStatus)
        }

        val data = getDfWithPeriodInName(session)

        val results = ConstraintSuggestionRunner()
          .onData(data)
          .addConstraintRules(Rules.DEFAULT)
          .addConstraintRule(UniqueIfApproximatelyUniqueRule())
          .run()

        val columns = data.columns.map { c =>
          escapeColumn(c)
        }

        val constraints: Seq[Constraint] = columns.flatMap { column =>
          results.constraintSuggestions
            .getOrElse(column, Seq())
            .map(suggestion => suggestion.constraint)
        }

        val checksToSucceed = Check(CheckLevel.Error, "group-1", constraints)

        assertStatusFor(data, checksToSucceed)(CheckStatus.Success)
      }
  }
}
