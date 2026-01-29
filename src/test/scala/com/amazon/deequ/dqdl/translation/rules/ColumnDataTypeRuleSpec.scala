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

import com.amazon.deequ.analyzers.FilteredRowOutcome
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.string.QuotedStringOperand
import software.amazon.glue.dqdl.model.condition.string.StringBasedCondition
import software.amazon.glue.dqdl.model.condition.string.StringBasedConditionOperator
import software.amazon.glue.dqdl.model.condition.string.StringOperand

import scala.jdk.CollectionConverters._

class ColumnDataTypeRuleSpec extends AnyWordSpec with Matchers {

  private def createStringCondition(operator: StringBasedConditionOperator,
                                    dataType: String): StringBasedCondition = {
    val operands: java.util.List[StringOperand] =
      List[StringOperand](new QuotedStringOperand(dataType)).asJava
    new StringBasedCondition(s"$operator $dataType", operator, operands)
  }

  "ColumnDataTypeRule" should {

    "convert EQUALS rule for DATE type" in {
      val parameters = Map("TargetColumn" -> "date_col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "DATE")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      result.right.get.column shouldBe "date_col"
    }

    "convert EQUALS rule for INTEGER type" in {
      val parameters = Map("TargetColumn" -> "int_col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "INTEGER")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      result.right.get.column shouldBe "int_col"
    }

    "convert EQUALS rule for DOUBLE type" in {
      val parameters = Map("TargetColumn" -> "double_col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "DOUBLE")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      result.right.get.column shouldBe "double_col"
    }

    "convert EQUALS rule for FLOAT type" in {
      val parameters = Map("TargetColumn" -> "float_col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "FLOAT")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      result.right.get.column shouldBe "float_col"
    }

    "convert EQUALS rule for LONG type" in {
      val parameters = Map("TargetColumn" -> "long_col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "LONG")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      result.right.get.column shouldBe "long_col"
    }

    "convert EQUALS rule for BOOLEAN type" in {
      val parameters = Map("TargetColumn" -> "bool_col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "BOOLEAN")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      result.right.get.column shouldBe "bool_col"
    }

    "convert EQUALS rule for TIMESTAMP type" in {
      val parameters = Map("TargetColumn" -> "ts_col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "TIMESTAMP")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      result.right.get.column shouldBe "ts_col"
    }

    "convert EQUALS rule for DECIMAL type with precision and scale" in {
      val parameters = Map("TargetColumn" -> "decimal_col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "DECIMAL(10,2)")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      result.right.get.column shouldBe "decimal_col"
    }

    "convert EQUALS rule for DECIMAL type with spaces" in {
      val parameters = Map("TargetColumn" -> "decimal_col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "DECIMAL(10, 2)")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
    }

    "convert NOT_EQUALS rule for DATE type" in {
      val parameters = Map("TargetColumn" -> "col")
      val condition = createStringCondition(StringBasedConditionOperator.NOT_EQUALS, "DATE")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
    }

    "fail for unrecognized data type" in {
      val parameters = Map("TargetColumn" -> "col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "UNKNOWN_TYPE")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isLeft shouldBe true
      result.left.get should include("Unrecognized data type")
    }

    "fail for unsupported operator IN" in {
      val parameters = Map("TargetColumn" -> "col")
      val condition = createStringCondition(StringBasedConditionOperator.IN, "DATE")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isLeft shouldBe true
      result.left.get should include("Unsupported operator")
    }

    "fail for unsupported operator NOT_IN" in {
      val parameters = Map("TargetColumn" -> "col")
      val condition = createStringCondition(StringBasedConditionOperator.NOT_IN, "DATE")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isLeft shouldBe true
      result.left.get should include("Unsupported operator")
    }

    "handle case-insensitive data type names" in {
      val parameters = Map("TargetColumn" -> "col")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "date")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
    }

    "handle column names with special characters" in {
      val parameters = Map("TargetColumn" -> "column-with-dashes")
      val condition = createStringCondition(StringBasedConditionOperator.EQUALS, "INTEGER")
      val rule = new DQRule("ColumnDataType", parameters.asJava, condition)

      val result = ColumnDataTypeRule.toExecutableRule(rule, FilteredRowOutcome.TRUE)

      result.isRight shouldBe true
      result.right.get.column shouldBe "column-with-dashes"
    }
  }
}
