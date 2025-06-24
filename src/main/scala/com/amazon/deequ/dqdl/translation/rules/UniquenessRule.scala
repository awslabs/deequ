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

package com.amazon.deequ.dqdl.translation.rules

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.dqdl.model.DeequMetricMapping
import com.amazon.deequ.dqdl.translation.DQDLRuleConverter
import com.amazon.deequ.dqdl.util.DQDLUtility.addWhereClause
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition

import scala.collection.JavaConverters._

case class UniquenessRule() extends DQDLRuleConverter {
  override def convert(rule: DQRule): Either[String, (Check, Seq[DeequMetricMapping])] = {
    val columns: Seq[String] = rule.getParameters.asScala.collect {
      case (k, v) if k.startsWith("TargetColumn") => v
    }.toSeq
    val check = Check(CheckLevel.Error, java.util.UUID.randomUUID.toString)
    columns match {
      case Nil => Left("Required parameters not found")

      case Seq(singleCol) =>
        val singleColCheck = check
          .hasUniqueness(singleCol, assertionAsScala(rule, rule.getCondition.asInstanceOf[NumberBasedCondition]))
        Right((addWhereClause(rule, singleColCheck),
          Seq(DeequMetricMapping("Column", singleCol, "Uniqueness", "Uniqueness", None, rule = rule))))

      case cols@(head +: tail) =>
        val multiColCheck = check
          .hasUniqueness(columns, assertionAsScala(rule, rule.getCondition.asInstanceOf[NumberBasedCondition]))
        Right(
          addWhereClause(rule, multiColCheck),
          Seq(DeequMetricMapping("Multicolumn", columns.mkString(","), "Uniqueness", "Uniqueness", None, rule = rule)))
    }
  }
}
