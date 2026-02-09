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

package com.amazon.deequ.dqdl

import com.amazon.deequ.dqdl.util.DefaultDQDLParser
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.glue.dqdl.parser.DQDLParser

import scala.collection.JavaConverters.collectionAsScalaIterableConverter


class DefaultDQDLParserTest extends AnyWordSpec with Matchers {

  val parser = new DQDLParser()

  "DQDL Parser" should {
    "parse valid DQDL rules" in {

      val ruleset = "Rules = [ RowCount > 1, ColumnCount = 3]"
      val dqRuleset = parser.parse(ruleset)
      val rules = dqRuleset.getRules.asScala

      // Test number of rules
      rules.size shouldBe 2

      // Test individual rules
      val rowCountRule = rules.find(_.getRuleType == "RowCount")
      rowCountRule.isDefined shouldBe true
      rowCountRule.map(_.toString) shouldBe Some("RowCount > 1")

      val columnCountRule = rules.find(_.getRuleType == "ColumnCount")
      columnCountRule.isDefined shouldBe true
      columnCountRule.map(_.toString) shouldBe Some("ColumnCount = 3")
    }

    "throw an IllegalArgumentException when DQDL can not be parsed" in {
      val thrown = intercept[IllegalArgumentException] {
        DefaultDQDLParser.parse("invalid")
      }
      thrown.getMessage should include("Parsing Error")
    }

  }
}
