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

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

class ColumnValuesRuleSpec extends AnyWordSpec with Matchers {

  "ColumnValuesRule" should {

    "convert GREATER_THAN numeric rule successfully" in {
      val parameters = Map("TargetColumn" -> "price")
      val rule = new DQRule("ColumnValues", parameters.asJava, ">10".asCondition)
      val columnValuesRule = ColumnValuesRule()

      val result = columnValuesRule.convert(rule)

      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("MinimumConstraint")
      metrics.size shouldBe 1
      metrics.head.name shouldBe "Minimum"
    }

    "convert LESS_THAN numeric rule successfully" in {
      val parameters = Map("TargetColumn" -> "quantity")
      val rule = new DQRule("ColumnValues", parameters.asJava, "<100".asCondition)
      val columnValuesRule = ColumnValuesRule()

      val result = columnValuesRule.convert(rule)

      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("MaximumConstraint")
      metrics.head.name shouldBe "Maximum"
    }

    "convert BETWEEN numeric rule with two constraints" in {
      val parameters = Map("TargetColumn" -> "age")
      val rule = new DQRule("ColumnValues", parameters.asJava, "between 18 and 65".asCondition)
      val columnValuesRule = ColumnValuesRule()

      val result = columnValuesRule.convert(rule)

      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 2
      check.toString should include("MinimumConstraint")
      check.toString should include("MaximumConstraint")
      metrics.size shouldBe 2
    }

    "convert numeric IN rule successfully" in {
      val parameters = Map("TargetColumn" -> "status_code")
      val rule = new DQRule("ColumnValues", parameters.asJava, "in [1,2,3]".asCondition)
      val columnValuesRule = ColumnValuesRule()

      val result = columnValuesRule.convert(rule)

      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("ComplianceConstraint")
      metrics.head.name shouldBe "ColumnValues.Compliance"
    }

    "handle column names requiring quotes" in {
      val parameters = Map("TargetColumn" -> "column-with-dashes")
      val rule = new DQRule("ColumnValues", parameters.asJava, ">5".asCondition)
      val columnValuesRule = ColumnValuesRule()

      val result = columnValuesRule.convert(rule)

      result.isRight shouldBe true
    }

    "convert rule with where clause" in {
      val parameters = Map("TargetColumn" -> "price")
      val rule = new DQRule("ColumnValues", parameters.asJava, ">0".asCondition, null, null,
        new java.util.ArrayList(), "category = 'electronics'")
      val columnValuesRule = ColumnValuesRule()

      val result = columnValuesRule.convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.toString should include("category = 'electronics'")
    }

    "convert EQUALS numeric rule successfully" in {
      val parameters = Map("TargetColumn" -> "flag")
      val rule = new DQRule("ColumnValues", parameters.asJava, "= 1".asCondition)
      val columnValuesRule = ColumnValuesRule()

      val result = columnValuesRule.convert(rule)

      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("ComplianceConstraint")
    }

    "convert GREATER_THAN_EQUAL_TO numeric rule successfully" in {
      val parameters = Map("TargetColumn" -> "score")
      val rule = new DQRule("ColumnValues", parameters.asJava, ">=0".asCondition)
      val columnValuesRule = ColumnValuesRule()

      val result = columnValuesRule.convert(rule)

      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("MinimumConstraint")
    }

    "convert LESS_THAN_EQUAL_TO numeric rule successfully" in {
      val parameters = Map("TargetColumn" -> "percentage")
      val rule = new DQRule("ColumnValues", parameters.asJava, "<=100".asCondition)
      val columnValuesRule = ColumnValuesRule()

      val result = columnValuesRule.convert(rule)

      result.isRight shouldBe true
      val (check, metrics) = result.right.get
      check.constraints.size shouldBe 1
      check.toString should include("MaximumConstraint")
    }
  }
}
