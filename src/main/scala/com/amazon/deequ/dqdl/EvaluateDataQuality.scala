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

package com.amazon.deequ.dqdl

import com.amazon.deequ.dqdl.execution.{DQDLExecutor, RowLevelResultHelper}
import com.amazon.deequ.dqdl.execution.executors.{CustomSqlRowLevelExecutor, DeequRulesExecutor}
import com.amazon.deequ.dqdl.model.{CompositeExecutableRule, CustomSqlRowLevelExecutableRule, DeequExecutableRule, ExecutableRule, RuleOutcome}
import com.amazon.deequ.dqdl.translation.{DQDLRuleTranslator, DeequOutcomeTranslator}
import com.amazon.deequ.dqdl.util.DefaultDQDLParser
import org.apache.spark.sql.DataFrame
import software.amazon.glue.dqdl.model.{DQRule, DQRuleset}

/**
 * Entry point for evaluating data quality.
 *
 * Given a Spark DataFrame and a DQDL ruleset (as a String), this object:
 *  - Parses and translates each rule from the ruleset to an ExecutableRule
 *  - Executes the rules on the DataFrame.
 *  - Translates the outcome back to a Spark DataFrame.
 */
object EvaluateDataQuality {

  val ORIGINAL_DATA_KEY = "originalData"
  val RULE_OUTCOMES_KEY = "ruleOutcomes"
  val ROW_LEVEL_OUTCOMES_KEY = "rowLevelOutcomes"

  /**
   * Validates the given Spark DataFrame against a set of data quality rules defined in DQDL format.
   *
   * @param df                the Spark DataFrame to analyze.
   * @param rulesetDefinition the data quality ruleset (defined in DQDL string format) to apply to the DataFrame.
   * @return a Spark DataFrame containing the aggregated data quality results.
   */
  def process(df: DataFrame, rulesetDefinition: String): DataFrame = {
    process(df, rulesetDefinition, Map.empty[String, DataFrame])
  }

  /**
   * Validates the given Spark DataFrame against a set of data quality rules defined in DQDL format.
   *
   * @param df                    the Spark DataFrame to analyze.
   * @param rulesetDefinition     the data quality ruleset (defined in DQDL string format) to apply to the DataFrame.
   * @param additionalDataSources A map of additional source aliases to their DataFrames.
   *                              Used for dataset comparison rules like RowCountMatch and SchemaMatch.
   * @return a Spark DataFrame containing the aggregated data quality results.
   */
  def process(df: DataFrame,
              rulesetDefinition: String,
              additionalDataSources: Map[String, DataFrame]): DataFrame = {
    val ruleset: DQRuleset = DefaultDQDLParser.parse(rulesetDefinition)
    val executableRules: Seq[ExecutableRule] = DQDLRuleTranslator.toExecutableRules(ruleset)
    val executedRulesResult = DQDLExecutor.executeRules(executableRules, df, additionalDataSources)
    DeequOutcomeTranslator.translate(executedRulesResult, df)
  }

  /**
   * Evaluates data quality rules and returns row-level results.
   *
   * @param df                the Spark DataFrame to analyze.
   * @param rulesetDefinition the data quality ruleset (defined in DQDL string format).
   * @return A map containing:
   *         - "originalData" -> the original DataFrame
   *         - "ruleOutcomes" -> DataFrame with overall rule outcomes
   *         - "rowLevelOutcomes" -> DataFrame with per-row pass/fail/skip arrays
   */
  def processRows(df: DataFrame, rulesetDefinition: String): Map[String, DataFrame] = {
    processRows(df, rulesetDefinition, Map.empty[String, DataFrame])
  }

  /**
   * Evaluates data quality rules and returns row-level results.
   *
   * @param df                    the Spark DataFrame to analyze.
   * @param rulesetDefinition     the data quality ruleset (defined in DQDL string format).
   * @param additionalDataSources A map of additional source aliases to their DataFrames.
   *                              Used for dataset comparison rules like RowCountMatch and SchemaMatch.
   * @return A map containing:
   *         - "originalData" -> the original DataFrame
   *         - "ruleOutcomes" -> DataFrame with overall rule outcomes
   *         - "rowLevelOutcomes" -> DataFrame with per-row pass/fail/skip arrays
   */
  def processRows(df: DataFrame,
                  rulesetDefinition: String,
                  additionalDataSources: Map[String, DataFrame]): Map[String, DataFrame] = {
    val ruleset: DQRuleset = DefaultDQDLParser.parse(rulesetDefinition)
    val executableRules: Seq[ExecutableRule] = DQDLRuleTranslator.toExecutableRules(ruleset)

    val allDeequRules = collectDeequRules(executableRules).distinct
    val deequResult = DeequRulesExecutor.executeWithRowLevel(allDeequRules, df)

    val (customSqlRules, remainingRules) = executableRules
      .filterNot(_.isInstanceOf[DeequExecutableRule])
      .partition(_.isInstanceOf[CustomSqlRowLevelExecutableRule])

    val customSqlResult = CustomSqlRowLevelExecutor.executeWithRowLevel(
      customSqlRules.map(_.asInstanceOf[CustomSqlRowLevelExecutableRule]),
      df, additionalDataSources, Some(deequResult.rowLevelData))

    val remainingOutcomes = if (remainingRules.nonEmpty) {
      DQDLExecutor.executeRules(remainingRules, df, additionalDataSources)
    } else {
      Map.empty[DQRule, RuleOutcome]
    }

    val allOutcomes = deequResult.outcomes ++ customSqlResult.outcomes ++ remainingOutcomes
    val ruleOutcomes = DeequOutcomeTranslator.translate(allOutcomes, df)

    val orderedOutcomes = executableRules.flatMap(r => allOutcomes.get(r.dqRule))
    val rowLevelOutcomes = RowLevelResultHelper.convert(customSqlResult.rowLevelData, orderedOutcomes)

    Map(
      ORIGINAL_DATA_KEY -> df,
      RULE_OUTCOMES_KEY -> ruleOutcomes,
      ROW_LEVEL_OUTCOMES_KEY -> rowLevelOutcomes
    )
  }

  private def collectDeequRules(rules: Seq[ExecutableRule]): Seq[DeequExecutableRule] = {
    rules.flatMap {
      case d: DeequExecutableRule => Seq(d)
      case c: CompositeExecutableRule => collectDeequRules(c.nestedRules)
      case _ => Seq.empty
    }
  }
}
