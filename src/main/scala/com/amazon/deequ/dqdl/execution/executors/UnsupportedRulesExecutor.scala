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

package com.amazon.deequ.dqdl.execution.executors

import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{Failed, RuleOutcome, UnsupportedExecutableRule}
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.DQRule

object UnsupportedRulesExecutor extends DQDLExecutor.RuleExecutor[UnsupportedExecutableRule] {

  override def executeRules(rules: Seq[UnsupportedExecutableRule], df: DataFrame): Map[DQRule, RuleOutcome] =
    rules.map { r =>
      val failureReason = "Rule (or nested rule) not supported" + r.reason.map(re => s" due to: $re").getOrElse("")
      r.dqRule -> RuleOutcome(r.dqRule, Failed, Some(failureReason))
    }.toMap

}
