/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import software.amazon.glue.dqdl.model.condition.string.Keyword
import software.amazon.glue.dqdl.model.condition.string.KeywordStringOperand
import software.amazon.glue.dqdl.model.condition.string.StringBasedCondition
import software.amazon.glue.dqdl.model.condition.string.StringBasedConditionOperator
import software.amazon.glue.dqdl.model.condition.string.StringOperand

import scala.jdk.CollectionConverters._

class ColumnValuesRuleSpec extends AnyWordSpec with Matchers {

  "ColumnValuesRule" should {

    "convert GREATER_THAN numeric rule with isComplete check" in {
      val parameters = Map("TargetColumn" -> "price")
      val rule = new DQRule("ColumnValues", parameters.asJava, "> 10".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 2
      check.constraints(0).toString should include("MinimumConstraint(Minimum(price")
      check.constraints(1).toString should include("CompletenessConstraint(Completeness(price")
    }

    "convert LESS_THAN numeric rule with isComplete check" in {
      val parameters = Map("TargetColumn" -> "quantity")
      val rule = new DQRule("ColumnValues", parameters.asJava, "< 100".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 2
      check.constraints(0).toString should include("MaximumConstraint(Maximum(quantity")
      check.constraints(1).toString should include("CompletenessConstraint(Completeness(quantity")
    }

    "convert GREATER_THAN_EQUAL_TO numeric rule with isComplete check" in {
      val parameters = Map("TargetColumn" -> "score")
      val rule = new DQRule("ColumnValues", parameters.asJava, ">= 0".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 2
      check.constraints(0).toString should include("MinimumConstraint(Minimum(score")
      check.constraints(1).toString should include("CompletenessConstraint(Completeness(score")
    }

    "convert LESS_THAN_EQUAL_TO numeric rule with isComplete check" in {
      val parameters = Map("TargetColumn" -> "percentage")
      val rule = new DQRule("ColumnValues", parameters.asJava, "<= 100".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 2
      check.constraints(0).toString should include("MaximumConstraint(Maximum(percentage")
      check.constraints(1).toString should include("CompletenessConstraint(Completeness(percentage")
    }

    "convert BETWEEN numeric rule with exclusive bounds and isComplete" in {
      val parameters = Map("TargetColumn" -> "age")
      val rule = new DQRule("ColumnValues", parameters.asJava, "between 18 and 65".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 3
      check.constraints(0).toString should include("MinimumConstraint(Minimum(age")
      check.constraints(1).toString should include("MaximumConstraint(Maximum(age")
      check.constraints(2).toString should include("CompletenessConstraint(Completeness(age")
    }

    "convert numeric IN rule with NULLs failing" in {
      val parameters = Map("TargetColumn" -> "status_code")
      val rule = new DQRule("ColumnValues", parameters.asJava, "in [1, 2, 3]".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("ComplianceConstraint")
      check.constraints(0).toString should include("status_code")
      check.constraints(0).toString should include("IS NOT NULL AND")
    }

    "convert numeric NOT IN rule with NULLs passing" in {
      val parameters = Map("TargetColumn" -> "error_code")
      val rule = new DQRule("ColumnValues", parameters.asJava, "not in [500, 503]".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("ComplianceConstraint")
      check.constraints(0).toString should include("error_code")
      check.constraints(0).toString should include("IS NULL OR")
    }

    "convert EQUALS numeric rule with isComplete check" in {
      val parameters = Map("TargetColumn" -> "flag")
      val rule = new DQRule("ColumnValues", parameters.asJava, "= 1".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 3
      check.constraints(0).toString should include("MinimumConstraint(Minimum(flag")
      check.constraints(1).toString should include("MaximumConstraint(Maximum(flag")
      check.constraints(2).toString should include("CompletenessConstraint(Completeness(flag")
    }

    "handle column names requiring quotes in SQL expressions" in {
      val parameters = Map("TargetColumn" -> "column-with-dashes")
      val rule = new DQRule("ColumnValues", parameters.asJava, "in [1, 2, 3]".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("ComplianceConstraint")
      check.constraints(0).toString should include("`column-with-dashes`")
    }

    "convert rule with where clause" in {
      val parameters = Map("TargetColumn" -> "price")
      val rule = new DQRule("ColumnValues", parameters.asJava, "> 0".asCondition, null, null,
        new java.util.ArrayList(), "category = 'electronics'")

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 2
      check.constraints(0).toString should include("MinimumConstraint(Minimum(price")
      check.constraints(0).toString should include("category = 'electronics'")
    }

    "convert string IN with NULL keyword" in {
      val parameters = Map("TargetColumn" -> "status")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new KeywordStringOperand(Keyword.NULL)).asJava
      val condition = new StringBasedCondition("in [NULL]",
        StringBasedConditionOperator.IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("ComplianceConstraint")
      check.constraints(0).toString should include("status")
      check.constraints(0).toString should include("IS NULL")
    }

    "convert string IN with EMPTY keyword" in {
      val parameters = Map("TargetColumn" -> "name")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new KeywordStringOperand(Keyword.EMPTY)).asJava
      val condition = new StringBasedCondition("in [EMPTY]",
        StringBasedConditionOperator.IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("ComplianceConstraint")
      check.constraints(0).toString should include("name")
      check.constraints(0).toString should include("= ''")
    }

    "convert string IN with WHITESPACES_ONLY keyword" in {
      val parameters = Map("TargetColumn" -> "description")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new KeywordStringOperand(Keyword.WHITESPACES_ONLY)).asJava
      val condition = new StringBasedCondition("in [WHITESPACES_ONLY]",
        StringBasedConditionOperator.IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("ComplianceConstraint")
      check.constraints(0).toString should include("LENGTH(TRIM(")
      check.constraints(0).toString should include("LENGTH(description) > 0")
    }

    "convert string NOT IN with NULL keyword (NULLs should fail)" in {
      val parameters = Map("TargetColumn" -> "status")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new KeywordStringOperand(Keyword.NULL)).asJava
      val condition = new StringBasedCondition("not in [NULL]",
        StringBasedConditionOperator.NOT_IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("ComplianceConstraint")
      check.constraints(0).toString should include("status")
      check.constraints(0).toString should include("IS NOT NULL")
    }

    "convert string NOT IN with EMPTY keyword" in {
      val parameters = Map("TargetColumn" -> "name")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new KeywordStringOperand(Keyword.EMPTY)).asJava
      val condition = new StringBasedCondition("not in [EMPTY]",
        StringBasedConditionOperator.NOT_IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("ComplianceConstraint")
      check.constraints(0).toString should include("name != ''")
    }

    "convert string NOT IN with WHITESPACES_ONLY keyword" in {
      val parameters = Map("TargetColumn" -> "description")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new KeywordStringOperand(Keyword.WHITESPACES_ONLY)).asJava
      val condition = new StringBasedCondition("not in [WHITESPACES_ONLY]",
        StringBasedConditionOperator.NOT_IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("ComplianceConstraint")
      check.constraints(0).toString should include("LENGTH(TRIM(")
    }
  }
}
