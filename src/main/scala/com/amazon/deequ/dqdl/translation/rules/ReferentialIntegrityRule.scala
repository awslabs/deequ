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

package com.amazon.deequ.dqdl.translation.rules

import com.amazon.deequ.dqdl.execution.DefaultOperandEvaluator
import com.amazon.deequ.dqdl.model.ReferentialIntegrityExecutableRule
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object ReferentialIntegrityRule {

  private val usingBracketsPattern: Regex = "(.+)\\.\\{(.+)\\}".r
  private val noBracketsPattern: Regex = "(.+)\\.([^\\.]+)$".r

  def toExecutableRule(rule: DQRule): Either[String, ReferentialIntegrityExecutableRule] = {
    val primaryCols = rule.getParameters.asScala("PrimaryDatasetColumns")
      .replaceAll("\"", "")
      .split(",")
      .map(_.trim)
      .toSeq

    val referenceAliasAndColumns = rule.getParameters.asScala("ReferenceDatasetColumns")
      .replaceAll("\"", "")

    val condition = rule.getCondition.asInstanceOf[NumberBasedCondition]
    val assertion: Double => Boolean = (d: Double) => condition.evaluate(d, rule, DefaultOperandEvaluator)

    referenceAliasAndColumns match {
      case usingBracketsPattern(alias, colsStr) =>
        val referenceCols = colsStr.split(",").map(_.trim).toSeq
        Right(ReferentialIntegrityExecutableRule(rule, primaryCols, alias, referenceCols, assertion))
      case noBracketsPattern(alias, col) =>
        Right(ReferentialIntegrityExecutableRule(rule, primaryCols, alias, Seq(col.trim), assertion))
      case _ =>
        Left("Could not parse ReferenceDatasetColumns parameter")
    }
  }
}
