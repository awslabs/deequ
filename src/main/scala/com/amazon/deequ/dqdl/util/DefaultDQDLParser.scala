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

import software.amazon.glue.dqdl.exception.InvalidDataQualityRulesetException
import software.amazon.glue.dqdl.model.DQRuleset
import software.amazon.glue.dqdl.parser.DQDLParser

import scala.util.{Failure, Success, Try}

trait DQDLParserTrait {
  def parse(ruleset: String): DQRuleset
}

object DefaultDQDLParser extends DQDLParserTrait {
  override def parse(ruleset: String): DQRuleset = {
    val dqdlParser: DQDLParser = new DQDLParser()
    val dqRuleset: DQRuleset = Try {
      dqdlParser.parse(ruleset)
    } match {
      case Success(value) => value
      case Failure(ex: InvalidDataQualityRulesetException) => throw new IllegalArgumentException(ex.getMessage)
      case Failure(ex) => throw ex
    }
    dqRuleset
  }
}
