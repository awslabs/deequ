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
import com.amazon.deequ.dqdl.model.DatasetMatchExecutableRule
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition

import scala.collection.JavaConverters._

object DatasetMatchRule {

  def toExecutableRule(rule: DQRule): Either[String, DatasetMatchExecutableRule] = {
    val params = rule.getParameters.asScala.mapValues(_.replaceAll("\"", ""))

    val referenceAlias = params("ReferenceDatasetAlias")
    val keyMappings = parseColumnMappings(params("KeyColumnMappings"))

    keyMappings match {
      case None => Left("Could not parse KeyColumnMappings")
      case Some(keyMap) =>
        val matchMappings = params.get("MatchColumnMappings").flatMap(parseColumnMappings)
        val condition = rule.getCondition.asInstanceOf[NumberBasedCondition]
        val assertion: Double => Boolean = d => condition.evaluate(d, rule, DefaultOperandEvaluator)
        Right(DatasetMatchExecutableRule(rule, referenceAlias, keyMap, matchMappings, assertion))
    }
  }

  private def parseColumnMappings(mappings: String): Option[Map[String, String]] = {
    if (mappings.isEmpty) return None
    Some(mappings.split(",").map { mapping =>
      val parts = mapping.trim.split("->")
      if (parts.length == 2) parts(0).trim -> parts(1).trim
      else parts(0).trim -> parts(0).trim
    }.toMap)
  }
}
