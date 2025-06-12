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

import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.ExecutableRule
import com.amazon.deequ.dqdl.translation.{DQDLRuleTranslator, DeequOutcomeTranslator}
import com.amazon.deequ.dqdl.util.DefaultDQDLParser
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.DQRuleset

/**
 * Entry point for evaluating data quality.
 *
 * Given a Spark DataFrame and a DQDL ruleset (as a String), this object:
 *  - Parses and translates each rule from the ruleset to an ExecutableRule
 *  - Executes the rules on the DataFrame.
 *  - Translates the outcome back to a Spark DataFrame.
 */
object EvaluateDataQuality {

  /**
   * Validates the given Spark DataFrame against a set of data quality rules defined in DQDL format.
   *
   * <p>This method applies the specified data quality ruleset to the input DataFrame and returns a new
   * DataFrame summarizing the overall quality results, including any issues detected during the analysis.</p>
   *
   * @param df                the Spark DataFrame to analyze.
   * @param rulesetDefinition the data quality ruleset (defined in DQDL string format) to apply to the DataFrame.
   * @return a Spark DataFrame containing the aggregated data quality results.
   */
  def process(df: DataFrame, rulesetDefinition: String): DataFrame = {
    // 1. Parse the ruleset
    val ruleset: DQRuleset = DefaultDQDLParser.parse(rulesetDefinition)

    // 2. Translate the dqRuleset into a corresponding ExecutableRules.
    val executableRules: Seq[ExecutableRule] = DQDLRuleTranslator.toExecutableRules(ruleset)

    // 3. Execute the rules against the DataFrame.
    val result = DQDLExecutor.executeRules(executableRules, df)

    // 4. Translate the results into a Spark DataFrame.
    val outcomeTranslator = new DeequOutcomeTranslator(df.sparkSession)
    outcomeTranslator.translate(result)
  }

}
