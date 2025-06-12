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

package com.amazon.deequ.dqdl.execution

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.dqdl.model.{ExecutableRule, Failed, Passed, RuleOutcome}
import com.amazon.deequ.dqdl.translation.DQDLRuleTranslator
import com.amazon.deequ.dqdl.util.DefaultDQDLParser
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.glue.dqdl.model.DQRule


class DQDLExecutorSpec extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "DQDLExecutor" should {
    "execute multiple RowCount rules with different outcomes" in
      withSparkSession { sparkSession =>
        // given
        val df = getDfFull(sparkSession)
        val ruleset = DefaultDQDLParser.parse("Rules=[RowCount < 10, RowCount < 3]")
        val rules = DQDLRuleTranslator.toExecutableRules(ruleset)

        // when
        val executedRules = DQDLExecutor.executeRules(rules, df)

        // then
        executedRules.size shouldBe 2

        val passingRule = findRuleByCondition(executedRules, "< 10")
        passingRule shouldBe defined
        val (passingDqRule, passingOutcome) = passingRule.get

        passingDqRule.getRuleType shouldBe "RowCount"
        passingOutcome.outcome shouldBe Passed
        passingOutcome.evaluatedMetrics.size shouldBe 1
        passingOutcome.evaluatedMetrics should contain key "Dataset.*.RowCount"
        passingOutcome.evaluatedMetrics.get("Dataset.*.RowCount") shouldBe Some(4.0)

        val failingRule = findRuleByCondition(executedRules, "< 3")
        failingRule shouldBe defined
        val (failingDqRule, failingOutcome) = failingRule.get

        failingDqRule.getRuleType shouldBe "RowCount"
        failingOutcome.outcome shouldBe Failed
        failingOutcome.evaluatedMetrics.size shouldBe 1
        failingOutcome.evaluatedMetrics should contain key "Dataset.*.RowCount"
        failingOutcome.evaluatedMetrics.get("Dataset.*.RowCount") shouldBe Some(4.0)
      }
  }

  private def findRuleByCondition(executedRules: Map[DQRule, RuleOutcome],
                                  condition: String): Option[(DQRule, RuleOutcome)] = {

    executedRules.find { case (dqRule, _) =>
      dqRule.toString.contains(condition)
    }
  }
}
