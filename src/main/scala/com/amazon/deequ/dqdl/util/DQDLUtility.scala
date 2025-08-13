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

package com.amazon.deequ.dqdl.util

import com.amazon.deequ.checks.{Check, CheckWithLastConstraintFilterable}
import software.amazon.glue.dqdl.model.DQRule

object DQDLUtility {

  def convertWhereClauseForMetric(whereClause: String): Option[String] =
    Option(whereClause).map(_ => s"(where: $whereClause)")

  def isWhereClausePresent(rule: DQRule): Boolean = {
    rule.getWhereClause != null
  }

  def addWhereClause(rule: DQRule, check: CheckWithLastConstraintFilterable): Check =
    if (isWhereClausePresent(rule)) check.where(rule.getWhereClause)
    else check

  def requiresToBeQuoted(s: String): Boolean = {
    if (s.startsWith("`") && s.endsWith("`")) false else {
      val specialCharsRegex = """[^a-zA-Z0-9]""".r
      specialCharsRegex.findFirstMatchIn(s).isDefined
    }
  }

}
