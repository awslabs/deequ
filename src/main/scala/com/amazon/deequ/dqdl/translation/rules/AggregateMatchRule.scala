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

import com.amazon.deequ.dqdl.execution.DefaultOperandEvaluator
import com.amazon.deequ.dqdl.model.{AggregateMatchExecutableRule, AggregateOperation, Avg, Sum, UnsupportedExecutableRule}
import com.amazon.deequ.dqdl.model.ExecutableRule
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object AggregateMatchRule {

  private val PrimaryAlias = "primary"
  // sum(colA), sum(reference-1.colA), avg(colB), avg(db.table.colB) etc.
  private val operationRegex: Regex = "(sum|avg|SUM|AVG)\\((.*)\\)".r

  def toExecutableRule(rule: DQRule): ExecutableRule = {
    val aggregateExpression1 = rule.getParameters.asScala("AggregateExpression1")
    val aggregateExpression2 = rule.getParameters.asScala("AggregateExpression2")
    val condition = rule.getCondition.asInstanceOf[NumberBasedCondition]
    val assertion: Double => Boolean =
      (d: Double) => condition.evaluate(d, rule, DefaultOperandEvaluator)

    val am = for {
      aggOp1 <- parseAggregateOperation(aggregateExpression1)
      aggOp2 <- parseAggregateOperation(aggregateExpression2)
    } yield AggregateMatchExecutableRule(rule, aggOp1, aggOp2, assertion)

    am match {
      case Some(r) => r
      case _ => UnsupportedExecutableRule(rule, Some("Unsupported Rule"))
    }
  }

  private def parseCol(c: String): (String, String) = {
    val splitAt = c.lastIndexOf(".")
    if (splitAt == -1) (c.replaceAll("\"", ""), PrimaryAlias)
    else {
      val column = c.substring(splitAt + 1)
      val datasourceAlias = c.substring(0, splitAt)
      (column.replaceAll("\"", ""), datasourceAlias)
    }
  }

  private def parseAggregateOperation(op: String): Option[AggregateOperation] = {
    op match {
      case operationRegex(aggOp, col) =>
        val (aggCol, refAlias) = parseCol(col)
        aggOp.toLowerCase match {
          case "avg" => Some(Avg(refAlias, aggCol))
          case "sum" => Some(Sum(refAlias, aggCol))
          case _ => None
        }
      case _ => None
    }
  }
}
