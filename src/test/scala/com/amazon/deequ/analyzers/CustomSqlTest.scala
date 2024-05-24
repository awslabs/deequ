/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Failure
import scala.util.Success

class CustomSqlTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "CustomSql" should {
    "return a metric when the statement returns exactly one value" in withSparkSession { session =>
      val data = getDfWithStringColumns(session)
      data.createOrReplaceTempView("primary")

      val sql = CustomSql("SELECT COUNT(*) FROM primary WHERE `Address Line 2` IS NOT NULL")
      val state = sql.computeStateFrom(data)
      val metric: DoubleMetric = sql.computeMetricFrom(state)

      metric.value.isSuccess shouldBe true
      metric.value.get shouldBe 6.0
    }

    "returns a failed metric when the statement returns more than one row" in withSparkSession { session =>
      val data = getDfWithStringColumns(session)
      data.createOrReplaceTempView("primary")

      val sql = CustomSql("Select `Address Line 2` FROM primary WHERE `Address Line 2` is NOT NULL")
      val state = sql.computeStateFrom(data)
      val metric = sql.computeMetricFrom(state)

      metric.value.isFailure shouldBe true
      metric.value match {
        case Success(_) => fail("Should have failed")
        case Failure(exception) => exception.getMessage shouldBe "Custom SQL did not return exactly 1 row"
      }
    }

    "returns a failed metric when the statement returns more than one column" in withSparkSession { session =>
      val data = getDfWithStringColumns(session)
      data.createOrReplaceTempView("primary")

      val sql = CustomSql(
        "Select `Address Line 1`, `Address Line 2` FROM primary WHERE `Address Line 3` like 'Bandra%'")
      val state = sql.computeStateFrom(data)
      val metric = sql.computeMetricFrom(state)

      metric.value.isFailure shouldBe true
      metric.value match {
        case Success(_) => fail("Should have failed")
        case Failure(exception) => exception.getMessage shouldBe "Custom SQL did not return exactly 1 column"
      }
    }

    "returns the error if the SQL statement has a syntax error" in withSparkSession { session =>
      val data = getDfWithStringColumns(session)
      data.createOrReplaceTempView("primary")

      val sql = CustomSql("Select `foo` from primary")
      val state = sql.computeStateFrom(data)
      val metric = sql.computeMetricFrom(state)

      metric.value.isFailure shouldBe true
      metric.value match {
        case Success(_) => fail("Should have failed")
        case Failure(exception) => exception.getMessage should include("foo")
      }
    }

    "apply metric disambiguation string to returned metric" in withSparkSession { session =>
      val data = getDfWithStringColumns(session)
      data.createOrReplaceTempView("primary")

      val disambiguator = "statement1"
      val sql = CustomSql("SELECT COUNT(*) FROM primary WHERE `Address Line 2` IS NOT NULL", disambiguator)
      val state = sql.computeStateFrom(data)
      val metric: DoubleMetric = sql.computeMetricFrom(state)

      metric.value.isSuccess shouldBe true
      metric.value.get shouldBe 6.0
      metric.name shouldBe "CustomSQL"
      metric.entity shouldBe Entity.Dataset
      metric.instance shouldBe "statement1"
    }
  }
}
