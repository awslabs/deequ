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
import com.amazon.deequ.dqdl.model.{AssertFailed, AssertIndeterminable, AssertPassed, CustomExecutableRule, Failed, Passed, RuleOutcome, UnsupportedExecutableRule}
import com.amazon.deequ.dqdl.translation.rules.CustomSqlRule
import com.amazon.deequ.dqdl.util.DQDLUtility
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import software.amazon.glue.dqdl.model.DQRule

object CustomRulesExecutor extends DQDLExecutor.RuleExecutor[CustomExecutableRule] {

  override def executeRules(rules: Seq[CustomExecutableRule], df: DataFrame): Map[DQRule, RuleOutcome] = {

    rules.map { r =>
      val outcome = r.dqRule.getRuleType match {
        case "CustomSql" => try {
          df.createOrReplaceTempView("primary")
          val sparkSession = df.sparkSession
          val dfSql = sparkSession.sql(r.customSqlStatement)
          val cols = dfSql.columns.toSeq
          cols match {
            case Seq(resultCol) =>
              val dfSqlCast = dfSql.withColumn(resultCol, col(s"`$resultCol`").cast(DoubleType))
              val results: Seq[Row] = dfSqlCast.collect()
              if (results.size != 1) {
                RuleOutcome(r.dqRule, Failed, Some("Custom SQL did not return exactly 1 row"))
              } else {
                val row = results.head
                val result: Double = row.get(0).asInstanceOf[Double]
                val sha256 = DQDLUtility.sha256Hash(r.customSqlStatement)
                val evaluatedMetrics = Map(
                  s"Dataset.*.CustomSQL" -> result,
                  s"Dataset.$sha256.CustomSQL" -> result)

                r.assertion(result) match {
                  case AssertPassed =>
                    RuleOutcome(r.dqRule, Passed, None, evaluatedMetrics)
                  case AssertFailed =>
                    RuleOutcome(r.dqRule, Failed, Some("Custom SQL response failed to satisfy the threshold"),
                      evaluatedMetrics)
                  case AssertIndeterminable(exceptMsg) =>
                    val msg = f"Custom SQL rule could not be evaluated; $exceptMsg"
                    RuleOutcome(r.dqRule, Failed, Some(msg), evaluatedMetrics)
                }
              }
            case _ => RuleOutcome(r.dqRule, Failed, Some("Custom SQL did not return exactly 1 column"))
          }
        } catch {
          case ex: Exception =>
            val failureReason = "Custom rule execution failed" +
              r.reason.map(re => s" due to: $re").getOrElse("") + s": ${ex.getMessage}"
            RuleOutcome(r.dqRule, Failed, Some(failureReason))
        }
        case _ => RuleOutcome(r.dqRule, Failed, Some(s"Unexpected custom rule type: ${r.dqRule.getRuleType}"))
      }

      r.dqRule -> outcome
    }.toMap
  }

}
