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

package com.amazon.deequ.dqdl.translation

import com.amazon.deequ.dqdl.model.DeequExecutableRule
import com.amazon.deequ.utils.ConditionUtils.ConditionAsString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.glue.dqdl.model.DQRule
import com.amazon.deequ.dqdl.model.UnsupportedExecutableRule
import com.amazon.deequ.dqdl.util.DefaultDQDLParser
import software.amazon.glue.dqdl.model.DQRuleset


import scala.jdk.CollectionConverters.mapAsJavaMapConverter


class DQDLRuleTranslatorSpec extends AnyWordSpec with Matchers {

  "DQDL rules translator" should {
    "translate RowCount rule" in {
      // given
      val parameters: Map[String, String] = Map.empty
      val rule: DQRule = new DQRule("RowCount", parameters.asJava, ">100".asCondition)

      // when
      val deequRuleOpt: Option[DeequExecutableRule] = DQDLRuleTranslator.translateRule(rule).toOption

      // then
      deequRuleOpt shouldBe defined
      deequRuleOpt.get.check.toString should include("SizeConstraint")
    }
  }

  "get executable rules for RowCount" in {
    // given
    val ruleset: DQRuleset = DefaultDQDLParser.parse("Rules=[RowCount > 10]")

    // when
    val rules = DQDLRuleTranslator.toExecutableRules(ruleset)

    // then
    rules.size should equal(1)
    val rule = rules.head
    rule.evaluatedMetricName.get should equal("Dataset.*.RowCount")
    rule.dqRule.getRuleType should equal("RowCount")
  }

  /*
  this test should be removed once all rules are supported. till then just put the next unimplemented rule here.
   */
  "get unknown executable rule" in {
    // given
    val ruleset: DQRuleset = DefaultDQDLParser
      .parse("Rules=[ColumnValues \"Foo\" = 5]")

    // when
    val rules = DQDLRuleTranslator.toExecutableRules(ruleset)

    // then
    rules.size should equal(1)
    val rule = rules.head
    rule shouldBe an[UnsupportedExecutableRule]
    rule.evaluatedMetricName should equal(None)
  }
}
