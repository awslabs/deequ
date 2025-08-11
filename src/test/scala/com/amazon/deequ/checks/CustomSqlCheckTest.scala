/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.checks

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.{SparkContextSpec, VerificationSuite}
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CustomSqlCheckTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "CustomSQL check" should {
    "succeed when assertion passes" in withSparkSession { session =>
      val df = getDfCompleteAndInCompleteColumns(session)
      df.createOrReplaceTempView("primary")

      val check = Check(CheckLevel.Error, "custom-sql-test")
        .customSql("SELECT COUNT(*) FROM primary", _ == 6.0)

      val result = VerificationSuite()
        .onData(df)
        .addCheck(check)
        .run()

      result.status shouldBe CheckStatus.Success
      
      val metricsDF = AnalyzerContext.successMetricsAsDataFrame(session, AnalyzerContext(result.metrics))
      val customSqlMetrics = metricsDF.filter(metricsDF("name") === "CustomSQL").collect()
      customSqlMetrics should have length 1
      customSqlMetrics(0).getAs[Double]("value") shouldBe 6.0
      customSqlMetrics(0).getAs[String]("entity") shouldBe "Dataset"
      customSqlMetrics(0).getAs[String]("instance") shouldBe "*"
    }

    "fail when assertion fails" in withSparkSession { session =>
      val df = getDfCompleteAndInCompleteColumns(session)
      df.createOrReplaceTempView("primary")

      val check = Check(CheckLevel.Error, "custom-sql-test")
        .customSql("SELECT COUNT(*) FROM primary", _ > 10.0)

      val result = VerificationSuite()
        .onData(df)
        .addCheck(check)
        .run()

      result.status shouldBe CheckStatus.Error

    }

    "work with complex SQL queries" in withSparkSession { session =>
      val df = getDfCompleteAndInCompleteColumns(session)
      df.createOrReplaceTempView("primary")

      val check = Check(CheckLevel.Error, "custom-sql-test")
        .customSql("SELECT COUNT(*) FROM primary WHERE att2 IS NOT NULL", _ == 4.0)

      val result = VerificationSuite()
        .onData(df)
        .addCheck(check)
        .run()

      result.status shouldBe CheckStatus.Success
    }
  }
}