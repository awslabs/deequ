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

package com.amazon.deequ.utils

import software.amazon.glue.dqdl.model.condition.Condition
import software.amazon.glue.dqdl.model.condition.number.{AtomicNumberOperand, NumberBasedCondition, NumberBasedConditionOperator, NumericOperand}

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object ConditionUtils {
  implicit class ConditionAsString(val expression: String) {
    private val dateRegex: Regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}|now\\(\\)".r

    def convertOperand(op: String): NumericOperand = new AtomicNumberOperand(op)

    def asCondition: Condition = {
      expression match {
        case e if dateRegex.findFirstIn(e).isDefined => new Condition(e)
        case e if e.startsWith("between") =>
          val operands = e
            .replace("between", "")
            .split("and")
            .toList
          new NumberBasedCondition(expression,
            NumberBasedConditionOperator.BETWEEN, operands.map(convertOperand).asJava)
        case e if e.startsWith(">=") =>
          val operand = e.replace(">=", "")
          new NumberBasedCondition(expression,
            NumberBasedConditionOperator.GREATER_THAN_EQUAL_TO, List(convertOperand(operand)).asJava)
        case e if e.startsWith(">") =>
          val operand = e.replace(">", "")
          new NumberBasedCondition(expression,
            NumberBasedConditionOperator.GREATER_THAN, List(convertOperand(operand)).asJava)
        case e if e.startsWith("<=") =>
          val operand = e.replace("<=", "")
          new NumberBasedCondition(expression,
            NumberBasedConditionOperator.LESS_THAN_EQUAL_TO, List(convertOperand(operand)).asJava)
        case e if e.startsWith("<") =>
          val operand = e.replace("<", "")
          new NumberBasedCondition(expression,
            NumberBasedConditionOperator.LESS_THAN, List(convertOperand(operand)).asJava)
        case e if e.startsWith("=") =>
          val operand = e.replace("=", "")
          new NumberBasedCondition(expression,
            NumberBasedConditionOperator.EQUALS, List(convertOperand(operand)).asJava)
        case e if e.startsWith("not in") =>
          val operandsStr = e.replace("not in", "").trim.stripPrefix("[").stripSuffix("]")
          val operands = operandsStr.split(",").map(_.trim).map(convertOperand).toList
          new NumberBasedCondition(expression,
            NumberBasedConditionOperator.NOT_IN, operands.asJava)
        case e if e.startsWith("in") =>
          val operandsStr = e.replace("in", "").trim.stripPrefix("[").stripSuffix("]")
          val operands = operandsStr.split(",").map(_.trim).map(convertOperand).toList
          new NumberBasedCondition(expression,
            NumberBasedConditionOperator.IN, operands.asJava)
        case e => new Condition(e)
      }
    }
  }
}
