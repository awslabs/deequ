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
import com.amazon.deequ.dqdl.model.CustomSqlRowLevelExecutableRule
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition

import scala.collection.JavaConverters._

object CustomSqlRowLevelRule {

  def toExecutableRule(rule: DQRule): CustomSqlRowLevelExecutableRule = {
    val statement = rule.getParameters.asScala("CustomSqlStatement")
    val assertion: Double => Boolean = Option(rule.getThresholdCondition)
      .filter(_.getConditionAsString.nonEmpty)
      .map(_.asInstanceOf[NumberBasedCondition])
      .map { t =>
        val evaluator = DefaultOperandEvaluator
        (d: Double) => t.evaluate(d, rule, evaluator).asInstanceOf[Boolean]
      }
      .getOrElse((d: Double) => d == 1.0)

    CustomSqlRowLevelExecutableRule(rule, statement, assertion)
  }
}
