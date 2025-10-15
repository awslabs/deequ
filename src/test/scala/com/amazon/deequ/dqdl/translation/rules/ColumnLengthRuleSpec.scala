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

package com.amazon.deequ.dqdl.translation.rules

import com.amazon.deequ.utils.ConditionUtils.ConditionAsString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.glue.dqdl.model.DQRule

import scala.jdk.CollectionConverters._

class ColumnLengthRuleSpec extends AnyWordSpec with Matchers {

  "ColumnLengthRule" should {

    "convert GREATER_THAN rule successfully" in {
      // given
      val parameters = Map("TargetColumn" -> "name")
      val rule = new DQRule("ColumnLength", parameters.asJava, ">5".asCondition)
      val columnLengthRule = ColumnLengthRule()

      // when
      val result = columnLengthRule.convert(rule)

      // then
      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("MinLengthConstraint")
      metrics.size shouldBe 3
      metrics.map(_.name) should contain allOf("MinimumLength", "MaximumLength", "LengthCompliance")
    }

    "convert LESS_THAN rule successfully" in {
      // given
      val parameters = Map("TargetColumn" -> "description")
      val rule = new DQRule("ColumnLength", parameters.asJava, "<100".asCondition)
      val columnLengthRule = ColumnLengthRule()

      // when
      val result = columnLengthRule.convert(rule)

      // then
      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("MaxLengthConstraint")
    }

    "convert BETWEEN rule with multiple constraints" in {
      // given
      val parameters = Map("TargetColumn" -> "title")
      val rule = new DQRule("ColumnLength", parameters.asJava, "between 5 and 50".asCondition)
      val columnLengthRule = ColumnLengthRule()

      // when
      val result = columnLengthRule.convert(rule)

      // then
      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 2
      check.toString should include("MinLengthConstraint")
      check.toString should include("MaxLengthConstraint")
    }

    "convert EQUALS rule with multiple constraints" in {
      // given
      val parameters = Map("TargetColumn" -> "code")
      val rule = new DQRule("ColumnLength", parameters.asJava, "=10".asCondition)
      val columnLengthRule = ColumnLengthRule()

      // when
      val result = columnLengthRule.convert(rule)

      // then
      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 2
      check.toString should include("MinLengthConstraint")
      check.toString should include("MaxLengthConstraint")
    }

    "handle column names requiring quotes" in {
      // given
      val parameters = Map("TargetColumn" -> "column-with-dashes")
      val rule = new DQRule("ColumnLength", parameters.asJava, ">5".asCondition)
      val columnLengthRule = ColumnLengthRule()

      // when
      val result = columnLengthRule.convert(rule)

      // then
      result.isRight shouldBe true
      // Should not fail with special characters in column name
    }

    "convert GREATER_THAN rule with where clause" in {
      // given
      val parameters = Map("TargetColumn" -> "name")
      val rule = new DQRule("ColumnLength", parameters.asJava, ">5".asCondition, null, null,
        new java.util.ArrayList(), "status = 'active'")
      val columnLengthRule = ColumnLengthRule()

      // when
      val result = columnLengthRule.convert(rule)

      // then
      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("status = 'active'")
    }

    "convert BETWEEN rule with where clause" in {
      // given
      val parameters = Map("TargetColumn" -> "title")
      val rule = new DQRule("ColumnLength", parameters.asJava, "between 5 and 50".asCondition, null, null,
        new java.util.ArrayList(), "category = 'book'")
      val columnLengthRule = ColumnLengthRule()

      // when
      val result = columnLengthRule.convert(rule)

      // then
      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 2
      check.toString should include("category = 'book'")
    }

    "convert IN rule successfully" in {
      // given
      val parameters = Map("TargetColumn" -> "status")
      val rule = new DQRule("ColumnLength", parameters.asJava, "in [5,10,15]".asCondition)
      val columnLengthRule = ColumnLengthRule()

      // when
      val result = columnLengthRule.convert(rule)

      // then
      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("ComplianceConstraint")
    }

    "convert NOT_IN rule successfully" in {
      // given
      val parameters = Map("TargetColumn" -> "code")
      val rule = new DQRule("ColumnLength", parameters.asJava, "not in [1,2,3]".asCondition)
      val columnLengthRule = ColumnLengthRule()

      // when
      val result = columnLengthRule.convert(rule)

      // then
      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("ComplianceConstraint")
    }
  }
}
