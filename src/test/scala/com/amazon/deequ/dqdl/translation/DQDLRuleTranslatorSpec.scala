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

import com.amazon.deequ.dqdl.model.{CustomExecutableRule, DeequExecutableRule, UnsupportedExecutableRule}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.amazon.deequ.dqdl.util.DefaultDQDLParser
import software.amazon.glue.dqdl.model.DQRuleset


class DQDLRuleTranslatorSpec extends AnyWordSpec with Matchers {

  "get DeequExecutableRule (e.g. RowCount)" in {
    // given
    val ruleset: DQRuleset = DefaultDQDLParser.parse("Rules=[RowCount > 10]")

    // when
    val rules = DQDLRuleTranslator.toExecutableRules(ruleset)

    // then
    rules.size should equal(1)
    val rule = rules.head
    rule shouldBe an[DeequExecutableRule]
    rule.evaluatedMetricName.get should equal("Dataset.*.RowCount")
    rule.dqRule.getRuleType should equal("RowCount")
  }

  "get CustomExecutableRule (e.g. CustomSql)" in {
    // given
    val ruleset: DQRuleset = DefaultDQDLParser
      .parse("Rules=[CustomSql \"select count(*) from primary\" between 10 and 20]")

    // when
    val rules = DQDLRuleTranslator.toExecutableRules(ruleset)

    // then
    rules.size should equal(1)
    val rule = rules.head
    rule shouldBe an[CustomExecutableRule]
    rule.evaluatedMetricName should equal(Some("Dataset.*.CustomSQL"))
  }

  /*
  this test can be removed once all rules are supported.
   */
  "get unknown executable rule" in {
    // given
    val ruleset: DQRuleset = DefaultDQDLParser
      .parse("Rules=[ColumnLength \"Foo\" = 5]")

    // when
    val rules = DQDLRuleTranslator.toExecutableRules(ruleset)

    // then
    rules.size should equal(1)
    val rule = rules.head
    rule shouldBe an[UnsupportedExecutableRule]
    rule.evaluatedMetricName should equal(None)
  }

}
