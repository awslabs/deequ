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

import com.amazon.deequ.analyzers.State
import com.amazon.deequ.metrics.Metric
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import software.amazon.glue.dqdl.model.DQRuleset
import software.amazon.glue.dqdl.model.condition.number.OperandEvaluator

object EvaluateDataQuality extends EvaluateDataQualityHelper {

  override def getOperandEvaluator(): OperandEvaluator = {
    new RuleOperandEvaluator()
  }

  /**
   * Validates the given Spark DataFrame against a set of data quality rules defined in DQDL format.
   *
   * <p>This method applies the specified data quality ruleset to the input DataFrame and returns a new
   * DataFrame summarizing the overall quality results, including any issues detected during the analysis.</p>
   *
   * @param df      the Spark DataFrame to analyze.
   * @param ruleset the data quality ruleset in DQDL format to apply to the DataFrame.
   * @return a Spark DataFrame containing the aggregated data quality results.
   */
  def process(df: DataFrame, ruleset: String): DataFrame = {

    // OperandEvaluator has to be instantiated exactly once since it is stateful
    val singletonEvaluator = getOperandEvaluator()

    val dqRuleTranslator = new DQDLRuleTranslator[State[_], Metric[_]] {
      override def operandEvaluator: OperandEvaluator = singletonEvaluator
    }

    val dqRuleset = DefaultDQDLParser.parse(ruleset)

    val schema = df.schema

    // Flatten and split the rules
    val flattened: FlattenedRuleset[State[_], Metric[_]] = flatten(dqRuleset, schema, dqRuleTranslator)

    // todo add execution of rules
    df
  }

}

/**
 * Encapsulating business logic into a trait for testing purposes
 */
private[dqdl] trait EvaluateDataQualityHelper {

  def getOperandEvaluator(): OperandEvaluator

  private[dqdl] def flatten(dqRuleset: DQRuleset,
                            schema: StructType,
                            dqRuleTranslator: DQDLRuleTranslator[State[_], Metric[_]]): FlattenedRuleset[State[_], Metric[_]] = {

    val flattenedRuleset = dqRuleTranslator.flattenRuleset(dqRuleset, schema)
    val deequRules = flattenedRuleset.deequRules

    val unsupportedRules = flattenedRuleset.unsupportedRules
    FlattenedRuleset(deequRules, unsupportedRules)
  }

}