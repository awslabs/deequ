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
import software.amazon.glue.dqdl.model.condition.number.{AtomicNumberOperand, NullNumericOperand, NumberBasedCondition, NumberBasedConditionOperator, NumericOperand}
import software.amazon.glue.dqdl.model.condition.string.{Keyword, KeywordStringOperand, QuotedStringOperand, StringBasedCondition, StringBasedConditionOperator, StringOperand}

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
      check.constraints(0).toString shouldBe "MinimumConstraint(Minimum(price,None,None))"
      check.constraints(1).toString shouldBe "CompletenessConstraint(Completeness(price,None,None))"
    }

    "convert LESS_THAN numeric rule with isComplete check" in {
      val parameters = Map("TargetColumn" -> "quantity")
      val rule = new DQRule("ColumnValues", parameters.asJava, "< 100".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 2
      check.constraints(0).toString shouldBe "MaximumConstraint(Maximum(quantity,None,None))"
      check.constraints(1).toString shouldBe "CompletenessConstraint(Completeness(quantity,None,None))"
    }

    "convert GREATER_THAN_EQUAL_TO numeric rule with isComplete check" in {
      val parameters = Map("TargetColumn" -> "score")
      val rule = new DQRule("ColumnValues", parameters.asJava, ">= 0".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 2
      check.constraints(0).toString shouldBe "MinimumConstraint(Minimum(score,None,None))"
      check.constraints(1).toString shouldBe "CompletenessConstraint(Completeness(score,None,None))"
    }

    "convert LESS_THAN_EQUAL_TO numeric rule with isComplete check" in {
      val parameters = Map("TargetColumn" -> "percentage")
      val rule = new DQRule("ColumnValues", parameters.asJava, "<= 100".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 2
      check.constraints(0).toString shouldBe "MaximumConstraint(Maximum(percentage,None,None))"
      check.constraints(1).toString shouldBe "CompletenessConstraint(Completeness(percentage,None,None))"
    }

    "convert BETWEEN numeric rule with exclusive bounds and isComplete" in {
      val parameters = Map("TargetColumn" -> "age")
      val rule = new DQRule("ColumnValues", parameters.asJava, "between 18 and 65".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 2
      check.constraints(0).toString should startWith("ComplianceConstraint(Compliance(age between 18.0 and 65.0")
      check.constraints(1).toString shouldBe "CompletenessConstraint(Completeness(age,None,None))"
    }

    "convert numeric IN rule with NULLs failing" in {
      val parameters = Map("TargetColumn" -> "status_code")
      val rule = new DQRule("ColumnValues", parameters.asJava, "in [1, 2, 3]".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include(
        "`status_code` IS NOT NULL AND `status_code` IN (1.0, 2.0, 3.0)")
    }

    "convert numeric NOT IN rule with NULLs passing" in {
      val parameters = Map("TargetColumn" -> "error_code")
      val rule = new DQRule("ColumnValues", parameters.asJava, "not in [500, 503]".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include(
        "`error_code` IS NULL OR `error_code` NOT IN (500.0, 503.0)")
    }

    "convert EQUALS numeric rule with isComplete check" in {
      val parameters = Map("TargetColumn" -> "flag")
      val rule = new DQRule("ColumnValues", parameters.asJava, "= 1".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 3
      check.constraints(0).toString shouldBe "MinimumConstraint(Minimum(flag,None,None))"
      check.constraints(1).toString shouldBe "MaximumConstraint(Maximum(flag,None,None))"
      check.constraints(2).toString shouldBe "CompletenessConstraint(Completeness(flag,None,None))"
    }

    "handle column names requiring quotes in SQL expressions" in {
      val parameters = Map("TargetColumn" -> "column-with-dashes")
      val rule = new DQRule("ColumnValues", parameters.asJava, "in [1, 2, 3]".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include(
        "`column-with-dashes` IS NOT NULL AND `column-with-dashes` IN (1.0, 2.0, 3.0)")
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
      check.constraints(0).toString should include("status IS NULL")
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
      check.constraints(0).toString should include("name = ''")
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
      check.constraints(0).toString should include(
        "(LENGTH(TRIM(description)) = 0 AND LENGTH(description) > 0)")
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
      check.constraints(0).toString should include("status IS NOT NULL")
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
      check.constraints(0).toString should include(
        "(LENGTH(TRIM(description)) > 0 OR LENGTH(description) = 0)")
    }

    "handle string IN with commas inside operand values" in {
      val parameters = Map("TargetColumn" -> "a")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](
          new QuotedStringOperand("1,2,3"),
          new QuotedStringOperand("4")
        ).asJava
      val condition = new StringBasedCondition("in [\"1,2,3\", \"4\"]",
        StringBasedConditionOperator.IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include("(a IS NOT NULL AND a IN ('1,2,3', '4'))")
    }

    "convert NOT_EQUALS numeric rule" in {
      val parameters = Map("TargetColumn" -> "status")
      val operands: java.util.List[NumericOperand] =
        List[NumericOperand](new AtomicNumberOperand("0")).asJava
      val condition = new NumberBasedCondition(
        "!= 0", NumberBasedConditionOperator.NOT_EQUALS, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include("status IS NULL OR status != 0.0")
    }

    "convert NOT_BETWEEN numeric rule" in {
      val parameters = Map("TargetColumn" -> "value")
      val operands: java.util.List[NumericOperand] = List[NumericOperand](
        new AtomicNumberOperand("10"),
        new AtomicNumberOperand("20")
      ).asJava
      val condition = new NumberBasedCondition(
        "not between 10 and 20", NumberBasedConditionOperator.NOT_BETWEEN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints.size shouldBe 1
      check.constraints(0).toString should include(
        "value IS NULL OR (value <= 10.0 OR value >= 20.0)")
    }

    "convert EQUALS NULL numeric rule" in {
      val parameters = Map("TargetColumn" -> "optional_field")
      val operands: java.util.List[NumericOperand] =
        List[NumericOperand](new NullNumericOperand("NULL")).asJava
      val condition = new NumberBasedCondition(
        "= NULL", NumberBasedConditionOperator.EQUALS, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include("`optional_field` IS NULL")
    }

    "convert NOT_EQUALS NULL numeric rule" in {
      val parameters = Map("TargetColumn" -> "required_field")
      val operands: java.util.List[NumericOperand] =
        List[NumericOperand](new NullNumericOperand("NULL")).asJava
      val condition = new NumberBasedCondition(
        "!= NULL", NumberBasedConditionOperator.NOT_EQUALS, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include("`required_field` IS NOT NULL")
    }

    "convert numeric IN with NULL keyword mixed" in {
      val parameters = Map("TargetColumn" -> "code")
      val operands: java.util.List[NumericOperand] = List[NumericOperand](
        new AtomicNumberOperand("1"),
        new AtomicNumberOperand("2"),
        new NullNumericOperand("NULL")
      ).asJava
      val condition = new NumberBasedCondition(
        "in [1, 2, NULL]", NumberBasedConditionOperator.IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include("code IN (1.0, 2.0) OR code IS NULL")
    }

    "convert numeric NOT IN with NULL keyword mixed" in {
      val parameters = Map("TargetColumn" -> "code")
      val operands: java.util.List[NumericOperand] = List[NumericOperand](
        new AtomicNumberOperand("99"),
        new NullNumericOperand("NULL")
      ).asJava
      val condition = new NumberBasedCondition(
        "not in [99, NULL]", NumberBasedConditionOperator.NOT_IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include("code NOT IN (99.0) AND code IS NOT NULL")
    }

    "convert string MATCHES rule" in {
      val parameters = Map("TargetColumn" -> "email")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new QuotedStringOperand(".*@.*\\.com")).asJava
      val condition = new StringBasedCondition(
        "matches \".*@.*\\.com\"", StringBasedConditionOperator.MATCHES, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString shouldBe "PatternMatchConstraint(email, ^.*@.*\\.com$)"
    }

    "convert string NOT_MATCHES rule" in {
      val parameters = Map("TargetColumn" -> "code")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new QuotedStringOperand("TEST.*")).asJava
      val condition = new StringBasedCondition(
        "not matches \"TEST.*\"", StringBasedConditionOperator.NOT_MATCHES, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString shouldBe "PatternMatchConstraint(code, ^(?!\\bTEST.*\\b).*$)"
    }

    "convert string EQUALS with quoted string" in {
      val parameters = Map("TargetColumn" -> "status")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new QuotedStringOperand("ACTIVE")).asJava
      val condition = new StringBasedCondition(
        "= \"ACTIVE\"", StringBasedConditionOperator.EQUALS, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include(
        "(status IS NOT NULL AND status IN ('ACTIVE'))")
    }

    "convert string NOT_EQUALS with quoted string" in {
      val parameters = Map("TargetColumn" -> "status")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new QuotedStringOperand("DELETED")).asJava
      val condition = new StringBasedCondition(
        "!= \"DELETED\"", StringBasedConditionOperator.NOT_EQUALS, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include(
        "(status IS NULL OR status NOT IN ('DELETED'))")
    }

    "handle string with single quotes (SQL escaping)" in {
      val parameters = Map("TargetColumn" -> "name")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new QuotedStringOperand("O'Brien")).asJava
      val condition = new StringBasedCondition(
        "= \"O'Brien\"", StringBasedConditionOperator.EQUALS, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include("(name IS NOT NULL AND name IN ('O''Brien'))")
    }

    "handle column names with spaces" in {
      val parameters = Map("TargetColumn" -> "column with spaces")
      val rule = new DQRule("ColumnValues", parameters.asJava, "in [1, 2]".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include(
        "`column with spaces` IS NOT NULL AND `column with spaces` IN (1.0, 2.0)")
    }

    "handle column names with dots" in {
      val parameters = Map("TargetColumn" -> "table.column")
      val rule = new DQRule("ColumnValues", parameters.asJava, "in [1, 2]".asCondition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include(
        "`table.column` IS NOT NULL AND `table.column` IN (1.0, 2.0)")
    }

    "return error for BETWEEN with insufficient operands" in {
      val parameters = Map("TargetColumn" -> "value")
      val operands: java.util.List[NumericOperand] = List[NumericOperand](
        new AtomicNumberOperand("10")
      ).asJava
      val condition = new NumberBasedCondition(
        "between 10", NumberBasedConditionOperator.BETWEEN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isLeft shouldBe true
      result.left.get shouldBe "BETWEEN requires two operands."
    }

    "return error for NOT_BETWEEN with insufficient operands" in {
      val parameters = Map("TargetColumn" -> "value")
      val operands: java.util.List[NumericOperand] = List[NumericOperand](
        new AtomicNumberOperand("10")
      ).asJava
      val condition = new NumberBasedCondition(
        "not between 10", NumberBasedConditionOperator.NOT_BETWEEN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isLeft shouldBe true
      result.left.get shouldBe "NOT BETWEEN requires two operands."
    }

    "NOT IN WHITESPACES_ONLY should pass for empty string" in {
      val parameters = Map("TargetColumn" -> "description")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new KeywordStringOperand(Keyword.WHITESPACES_ONLY)).asJava
      val condition = new StringBasedCondition(
        "not in [WHITESPACES_ONLY]", StringBasedConditionOperator.NOT_IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include(
        "(LENGTH(TRIM(description)) > 0 OR LENGTH(description) = 0)")
    }

    "IN WHITESPACES_ONLY should fail for empty string" in {
      val parameters = Map("TargetColumn" -> "description")
      val operands: java.util.List[StringOperand] =
        List[StringOperand](new KeywordStringOperand(Keyword.WHITESPACES_ONLY)).asJava
      val condition = new StringBasedCondition(
        "in [WHITESPACES_ONLY]", StringBasedConditionOperator.IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include(
        "(LENGTH(TRIM(description)) = 0 AND LENGTH(description) > 0)")
    }

    "return error for empty numeric operands" in {
      val parameters = Map("TargetColumn" -> "value")
      val operands: java.util.List[NumericOperand] = List[NumericOperand]().asJava
      val condition = new NumberBasedCondition(
        "> ", NumberBasedConditionOperator.GREATER_THAN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isLeft shouldBe true
      result.left.get shouldBe "ColumnValues rule requires at least one operand."
    }

    "handle string IN with mixed keywords and quoted values" in {
      val parameters = Map("TargetColumn" -> "status")
      val operands: java.util.List[StringOperand] = List[StringOperand](
        new KeywordStringOperand(Keyword.NULL),
        new KeywordStringOperand(Keyword.EMPTY),
        new QuotedStringOperand("active")
      ).asJava
      val condition = new StringBasedCondition(
        "in [NULL, EMPTY, \"active\"]", StringBasedConditionOperator.IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      val sql = check.constraints(0).toString
      sql should include("status IS NULL")
      sql should include("status = ''")
      sql should include("'active'")
    }

    "convert numeric IN with only NULL keyword" in {
      val parameters = Map("TargetColumn" -> "code")
      val operands: java.util.List[NumericOperand] = List[NumericOperand](
        new NullNumericOperand("NULL")
      ).asJava
      val condition = new NumberBasedCondition(
        "in [NULL]", NumberBasedConditionOperator.IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include("code IS NULL")
    }

    "convert numeric NOT IN with only NULL keyword" in {
      val parameters = Map("TargetColumn" -> "code")
      val operands: java.util.List[NumericOperand] = List[NumericOperand](
        new NullNumericOperand("NULL")
      ).asJava
      val condition = new NumberBasedCondition(
        "not in [NULL]", NumberBasedConditionOperator.NOT_IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include("code IS NOT NULL")
    }

    "convert string NOT IN with quoted values" in {
      val parameters = Map("TargetColumn" -> "status")
      val operands: java.util.List[StringOperand] = List[StringOperand](
        new QuotedStringOperand("inactive"),
        new QuotedStringOperand("deleted")
      ).asJava
      val condition = new StringBasedCondition(
        "not in [\"inactive\", \"deleted\"]", StringBasedConditionOperator.NOT_IN, operands)
      val rule = new DQRule("ColumnValues", parameters.asJava, condition)

      val result = ColumnValuesRule().convert(rule)

      result.isRight shouldBe true
      val (check, _) = result.right.get
      check.constraints(0).toString should include(
        "status IS NULL OR status NOT IN ('inactive', 'deleted')")
    }
  }
}
