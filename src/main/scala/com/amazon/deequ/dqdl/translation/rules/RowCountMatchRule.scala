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
import com.amazon.deequ.dqdl.model.RowCountMatchExecutableRule
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition

import scala.collection.JavaConverters._

object RowCountMatchRule {
  def toExecutableRule(rule: DQRule): RowCountMatchExecutableRule = {
    val referenceDatasetAlias = rule.getParameters.asScala("ReferenceDatasetAlias")
    val condition = rule.getCondition.asInstanceOf[NumberBasedCondition]
    val assertion: Double => Boolean = (d: Double) => condition.evaluate(d, rule, DefaultOperandEvaluator)
    RowCountMatchExecutableRule(rule, referenceDatasetAlias, assertion)
  }
}
