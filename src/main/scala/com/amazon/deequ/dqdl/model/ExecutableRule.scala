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

package com.amazon.deequ.dqdl.model

import com.amazon.deequ.analyzers.FilteredRowOutcome.FilteredRowOutcome
import com.amazon.deequ.checks.Check
import com.amazon.deequ.dqdl.util.DQDLUtility.convertWhereClauseForMetric
import org.apache.spark.sql.Column
import software.amazon.glue.dqdl.model.{DQRule, DQRuleLogicalOperator}

trait ExecutableRule {
  val dqRule: DQRule
  val evaluatedMetricName: Option[String]
}

case class UnsupportedExecutableRule(dqRule: DQRule, reason: Option[String] = None) extends ExecutableRule {
  override val evaluatedMetricName: Option[String] = None
}

case class DeequExecutableRule(dqRule: DQRule,
                               check: Check,
                               deequMetricMappings: Seq[DeequMetricMapping] = Seq.empty) extends ExecutableRule {

  private val Delim = "."
  val evaluatedMetricName: Option[String] = deequMetricMappings match {
    case mappings if mappings.size == 1 =>
      Some(s"${mappings.head.entity}$Delim${mappings.head.instance}$Delim${mappings.head.name}")
    case _ => None // Multiple metrics reported for rule; cannot determine name for general case
  }
}

case class RowCountMatchExecutableRule(dqRule: DQRule,
                                       referenceDatasetAlias: String,
                                       assertion: Double => Boolean) extends ExecutableRule {
  override val evaluatedMetricName: Option[String] =
    Some(s"Dataset.$referenceDatasetAlias.RowCountMatch")
}

case class DataFreshnessExecutableRule(dqRule: DQRule,
                                       column: String,
                                       outcomeExpression: Column,
                                       filteredRow: FilteredRowOutcome) extends ExecutableRule {
  override val evaluatedMetricName: Option[String] =
    Some(s"Column.$column.DataFreshness.Compliance")
}

case class ReferentialIntegrityExecutableRule(dqRule: DQRule,
                                              primaryColumns: Seq[String],
                                              referenceDatasetAlias: String,
                                              referenceColumns: Seq[String],
                                              assertion: Double => Boolean) extends ExecutableRule {
  override val evaluatedMetricName: Option[String] =
    Some(s"Column.$referenceDatasetAlias.ReferentialIntegrity")
}

case class SchemaMatchExecutableRule(dqRule: DQRule,
                                     referenceDatasetAlias: String,
                                     assertion: Double => Boolean) extends ExecutableRule {
  override val evaluatedMetricName: Option[String] =
    Some(s"Dataset.$referenceDatasetAlias.SchemaMatch")
}

case class DatasetMatchExecutableRule(dqRule: DQRule,
                                      referenceDatasetAlias: String,
                                      keyColumnMappings: Map[String, String],
                                      matchColumnMappings: Option[Map[String, String]],
                                      assertion: Double => Boolean) extends ExecutableRule {
  override val evaluatedMetricName: Option[String] =
    Some(s"Dataset.$referenceDatasetAlias.DatasetMatch")
}

case class ColumnNamesMatchPatternExecutableRule(dqRule: DQRule,
                                                 pattern: String) extends ExecutableRule {
  override val evaluatedMetricName: Option[String] =
    Some("Dataset.*.ColumnNamesPatternMatchRatio")
}

/**
 * Represents a composite rule that combines multiple nested rules using logical operators (AND/OR).
 * Composite rules allow complex data quality checks by composing simpler rules.
 *
 * Example: `(RowCount > 0) and (IsComplete "column")`
 *
 * Note: Currently only supports dataset-level evaluation. Row-level evaluation is not yet implemented.
 *
 * @param dqRule The DQDL rule definition
 * @param nestedRules The child rules to be evaluated and combined
 * @param operator The logical operator (AND/OR) used to combine nested rule outcomes
 */
case class CompositeExecutableRule(dqRule: DQRule,
                                   nestedRules: Seq[ExecutableRule],
                                   operator: DQRuleLogicalOperator) extends ExecutableRule {
  override val evaluatedMetricName: Option[String] = None
}

sealed trait AggregateOperation {
  def dataSourceAlias: String
  def column: String
}

case class Sum(dataSourceAlias: String, column: String) extends AggregateOperation
case class Avg(dataSourceAlias: String, column: String) extends AggregateOperation

case class AggregateMatchExecutableRule(dqRule: DQRule,
                                        firstAggregateOperation: AggregateOperation,
                                        secondAggregateOperation: AggregateOperation,
                                        assertion: Double => Boolean) extends ExecutableRule {
  override val evaluatedMetricName: Option[String] = {
    val col1 = firstAggregateOperation.column
    val col2 = secondAggregateOperation.column
    val instance = if (col1 == col2) col1 else s"$col1,$col2"
    Some(s"Column.$instance.AggregateMatch")
  }
}

case class DeequMetricMapping(entity: String,
                              instance: String,
                              name: String,
                              deequName: String,
                              deequInstance: Option[String] = None,
                              disambiguator: Option[String] = None)

object DeequMetricMapping {
  def apply(entity: String,
            instance: String,
            name: String,
            deequName: String,
            deequInstance: Option[String],
            rule: DQRule): DeequMetricMapping = {
    new DeequMetricMapping(
      entity,
      instance,
      name,
      deequName,
      deequInstance,
      convertWhereClauseForMetric(rule.getWhereClause)
    )
  }
}
