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

package com.amazon.deequ.dqdl.execution.executors

import com.amazon.deequ.dqdl.execution.DQDLExecutor
import com.amazon.deequ.dqdl.model.{AggregateMatchExecutableRule, AggregateOperation, Avg, Failed, Passed, RuleOutcome, Sum}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, sum}
import org.apache.spark.sql.types.DoubleType
import software.amazon.glue.dqdl.model.DQRule

import scala.util.{Failure, Success, Try}

object AggregateMatchExecutor extends DQDLExecutor.RuleExecutor[AggregateMatchExecutableRule] {

  private val PrimaryAlias = "primary"

  override def executeRules(rules: Seq[AggregateMatchExecutableRule], df: DataFrame,
                            additionalDataSources: Map[String, DataFrame] = Map.empty): Map[DQRule, RuleOutcome] = {
    val dataSources = additionalDataSources + (PrimaryAlias -> df)
    rules.map { rule =>
      rule.dqRule -> evaluateRule(rule, dataSources)
    }.toMap
  }

  private def evaluateRule(rule: AggregateMatchExecutableRule,
                           dataSources: Map[String, DataFrame]): RuleOutcome = {
    val result = for {
      first <- evaluateAggregate(rule.firstAggregateOperation, dataSources)
      second <- evaluateAggregate(rule.secondAggregateOperation, dataSources)
    } yield computeRatio(first, second)

    result match {
      case Right(ratio) =>
        val metricName = rule.evaluatedMetricName.get
        val metrics = Map(metricName -> ratio)
        if (rule.assertion(ratio)) {
          RuleOutcome(rule.dqRule, Passed, None, metrics)
        } else {
          RuleOutcome(rule.dqRule, Failed,
            Some(s"Value: $ratio does not meet the constraint requirement."), metrics)
        }
      case Left(errorMsg) =>
        RuleOutcome(rule.dqRule, Failed, Some(errorMsg))
    }
  }

  private def evaluateAggregate(op: AggregateOperation,
                                dataSources: Map[String, DataFrame]): Either[String, Double] = {
    dataSources.get(op.dataSourceAlias) match {
      case Some(ds) =>
        val colOp = op match {
          case Avg(_, _) => avg(op.column).cast(DoubleType)
          case Sum(_, _) => sum(op.column).cast(DoubleType)
        }
        Try(ds.select(colOp).collect().head.getAs[Double](0)) match {
          case Success(v) => Right(v)
          case Failure(ex) => Left(s"Exception: ${ex.getClass.getName}")
        }
      case None =>
        Left(s"${op.dataSourceAlias} not found in additional sources")
    }
  }

  private def computeRatio(first: Double, second: Double): Double = {
    if (first == 0 && second == 0) 1.0 else if (second == 0) 0.0 else first / second
  }
}
