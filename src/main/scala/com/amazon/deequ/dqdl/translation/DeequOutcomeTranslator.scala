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

import com.amazon.deequ.dqdl.model.{Failed, RuleOutcome}
import org.apache.spark.sql.{DataFrame, SparkSession}
import software.amazon.glue.dqdl.model.{DQRule, DQRuleLogicalOperator}

/**
 * Translates the outcome of Deequ checks (RuleOutcome)
 * into a Spark DataFrame containing the results.
 */
object DeequOutcomeTranslator {

  def translate(executedRulesResult: Map[DQRule, RuleOutcome], df: DataFrame): DataFrame = {

    // Reuse SparkSession from the existing DataFrame
    val spark = df.sparkSession
    import spark.implicits._

    executedRulesResult.values.toSeq.map { r =>
      (
        r.rule.toString,
        r.outcome.asString,
        r.failureReason.orNull,
        r.evaluatedMetrics,
        r.evaluatedRule.map(_.toString).orNull
      )
    }.toDF("Rule", "Outcome", "FailureReason", "EvaluatedMetrics", "EvaluatedRule")
  }

}
