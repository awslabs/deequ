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

package com.amazon.deequ.dqdl.translation

import com.amazon.deequ.analyzers.FilteredRowOutcome
import com.amazon.deequ.dqdl.model.{CompositeExecutableRule, DeequExecutableRule, ExecutableRule, UnsupportedExecutableRule}
import com.amazon.deequ.dqdl.translation.rules.ColumnCorrelationRule
import com.amazon.deequ.dqdl.translation.rules.CompletenessRule
import com.amazon.deequ.dqdl.translation.rules.CustomSqlRule
import com.amazon.deequ.dqdl.translation.rules.DistinctValuesCountRule
import com.amazon.deequ.dqdl.translation.rules.EntropyRule
import com.amazon.deequ.dqdl.translation.rules.IsCompleteRule
import com.amazon.deequ.dqdl.translation.rules.IsPrimaryKeyRule
import com.amazon.deequ.dqdl.translation.rules.IsUniqueRule
import com.amazon.deequ.dqdl.translation.rules.MeanRule
import com.amazon.deequ.dqdl.translation.rules.ColumnCountRule
import com.amazon.deequ.dqdl.translation.rules.RowCountRule
import com.amazon.deequ.dqdl.translation.rules.StandardDeviationRule
import com.amazon.deequ.dqdl.translation.rules.SumRule
import com.amazon.deequ.dqdl.translation.rules.UniqueValueRatioRule
import com.amazon.deequ.dqdl.translation.rules.UniquenessRule
import com.amazon.deequ.dqdl.translation.rules.ColumnLengthRule
import com.amazon.deequ.dqdl.translation.rules.ColumnExistsRule
import com.amazon.deequ.dqdl.translation.rules.ColumnValuesRule
import com.amazon.deequ.dqdl.translation.rules.RowCountMatchRule
import com.amazon.deequ.dqdl.translation.rules.ReferentialIntegrityRule
import com.amazon.deequ.dqdl.translation.rules.DatasetMatchRule
import com.amazon.deequ.dqdl.translation.rules.DataFreshnessRule
import com.amazon.deequ.dqdl.translation.rules.ColumnNamesMatchPatternRule
import com.amazon.deequ.dqdl.translation.rules.SchemaMatchRule
import com.amazon.deequ.dqdl.translation.rules.AggregateMatchRule
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.DQRuleset

import scala.collection.JavaConverters.collectionAsScalaIterableConverter


/**
 * Translates DQDL rules into ExecutableRules.
 * Allows registration of specific converters for different rule types.
 */
object DQDLRuleTranslator {

  // Map from rule type to its converter implementation.
  private val converters = Map[String, DQDLRuleConverter](
    "RowCount" -> new RowCountRule,
    "ColumnCount" -> new ColumnCountRule,
    "Completeness" -> new CompletenessRule,
    "IsComplete" -> new IsCompleteRule,
    "Uniqueness" -> new UniquenessRule,
    "IsUnique" -> new IsUniqueRule,
    "ColumnCorrelation" -> new ColumnCorrelationRule,
    "DistinctValuesCount" -> new DistinctValuesCountRule,
    "Entropy" -> new EntropyRule,
    "Mean" -> new MeanRule,
    "StandardDeviation" -> new StandardDeviationRule,
    "Sum" -> new SumRule,
    "UniqueValueRatio" -> new UniqueValueRatioRule,
    "CustomSql" -> new CustomSqlRule,
    "IsPrimaryKey" -> new IsPrimaryKeyRule,
    "ColumnLength" -> new ColumnLengthRule,
    "ColumnExists" -> new ColumnExistsRule,
    "ColumnValues" -> new ColumnValuesRule
  )

  /**
   * Translates a single DQDL rule
   */
  private[dqdl] def translateRule(rule: DQRule): Either[String, DeequExecutableRule] = {
    converters.get(rule.getRuleType) match {
      case None =>
        Left(s"No converter found for rule type: ${rule.getRuleType}")
      case Some(converter) =>
        converter.convert(rule) map { case (check, deequMetrics) => DeequExecutableRule(rule, check, deequMetrics) }
    }
  }

  private[dqdl] def toExecutableRule(rule: DQRule): ExecutableRule = {
    rule.getRuleType match {
      case "Composite" =>
        // Validate nested rules exist
        if (rule.getNestedRules == null || rule.getNestedRules.isEmpty) {
          UnsupportedExecutableRule(rule, Some("Composite rule must have at least one nested rule"))
        } else {
          // Recursively translate nested rules
          val nestedExecutableRules = rule.getNestedRules.asScala.map(toExecutableRule).toSeq
          CompositeExecutableRule(rule, nestedExecutableRules, rule.getOperator)
        }

      case "DataFreshness" =>
        DataFreshnessRule.toExecutableRule(rule, FilteredRowOutcome.TRUE) match {
          case Right(executableRule) => executableRule
          case Left(message) => UnsupportedExecutableRule(rule, Some(message))
        }
      case "RowCountMatch" => RowCountMatchRule.toExecutableRule(rule)
      case "ColumnNamesMatchPattern" => ColumnNamesMatchPatternRule.toExecutableRule(rule)
      case "AggregateMatch" => AggregateMatchRule.toExecutableRule(rule)
      case "ReferentialIntegrity" =>
        ReferentialIntegrityRule.toExecutableRule(rule) match {
          case Right(executableRule) => executableRule
          case Left(message) => UnsupportedExecutableRule(rule, Some(message))
        }
      case "SchemaMatch" => SchemaMatchRule.toExecutableRule(rule)
      case "DatasetMatch" =>
        DatasetMatchRule.toExecutableRule(rule) match {
          case Right(executableRule) => executableRule
          case Left(message) => UnsupportedExecutableRule(rule, Some(message))
        }
      case _ =>
        translateRule(rule) match {
          case Right(deequExecutableRule) => deequExecutableRule
          case Left(message) => UnsupportedExecutableRule(rule, Some(message))
        }
    }
  }

  /**
   * Translate a ruleset to executable rules
   */
  def toExecutableRules(ruleset: DQRuleset): Seq[ExecutableRule] = {
    ruleset.getRules.asScala.map(toExecutableRule).toSeq.distinct
  }

}
