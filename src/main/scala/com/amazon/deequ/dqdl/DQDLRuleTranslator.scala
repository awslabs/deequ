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

import com.amazon.deequ.checks.Check
import software.amazon.glue.dqdl.model.DQRule


/**
 * Translates DQDL rules into Deequ Checks.
 * Allows registration of specific converters for different rule types.
 */
object DQDLRuleTranslator {

  // Map from rule type to its converter implementation.
  private var converters: Map[String, DQDLRuleConverter] = Map.empty

  register("Completeness", new CompletenessRuleConverter)
  register("RowCount", new RowCountRuleConverter)

  private def register(ruleType: String, converter: DQDLRuleConverter): Unit = {
    converters += (ruleType -> converter)
  }

  /**
   * Translates a single DQDL rule into an optional Deequ Check.
   */
  def translateRule(rule: DQRule): Option[Check] = {
    converters.get(rule.getRuleType).flatMap(_.translate(rule))
  }
}
