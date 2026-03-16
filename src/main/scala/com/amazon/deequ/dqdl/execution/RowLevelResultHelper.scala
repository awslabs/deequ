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

package com.amazon.deequ.dqdl.execution

import com.amazon.deequ.dqdl.model._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import software.amazon.glue.dqdl.model.{DQRule, DQRuleLogicalOperator}

object RowLevelResultHelper {

  val ROW_LEVEL_OUTCOME_COLUMN = "DataQualityEvaluationResult"
  val ROW_LEVEL_PASS = "DataQualityRulesPass"
  val ROW_LEVEL_FAIL = "DataQualityRulesFail"
  val ROW_LEVEL_SKIP = "DataQualityRulesSkip"

  def convert(data: DataFrame, ruleOutcomes: Seq[RuleOutcome]): DataFrame = {
    val rulesetOutcomes = composeRuleOutcomes(data, ruleOutcomes)
    val columnsToRules = mapColumnsToRules(rulesetOutcomes, ruleOutcomes)
    val allRules = ruleOutcomes.map(_.rule)
    val booleanColumnNames = columnsToRules.keys.toSeq

    if (booleanColumnNames.isEmpty) {
      return rulesetOutcomes
        .withColumn(ROW_LEVEL_PASS, typedLit(Seq.empty[String]))
        .withColumn(ROW_LEVEL_FAIL, typedLit(Seq.empty[String]))
        .withColumn(ROW_LEVEL_SKIP, typedLit(allRules.map(_.toString)))
        .withColumn(ROW_LEVEL_OUTCOME_COLUMN, lit(Passed.asString))
    }

    val withStringCols = booleanColumnNames.foldLeft(rulesetOutcomes) { (df, c) =>
      df.withColumn(c, col(c).cast("string"))
    }

    val uuidToRule = columnsToRules.map { case (uuid, rule) => uuid -> rule.toString }

    val allRulesMap: Map[String, String] = allRules.map { rule =>
      columnsToRules.find(_._2 == rule) match {
        case Some((uuid, _)) => uuid -> rule.toString
        case None => java.util.UUID.randomUUID().toString -> rule.toString
      }
    }.toMap

    def conditionColumn(condition: String): Column = {
      val conditions = booleanColumnNames.map { uuid =>
        when(col(uuid) === condition, lit(uuidToRule.getOrElse(uuid, uuid)))
          .otherwise("__placeholder__")
      }
      array_remove(array(conditions: _*), "__placeholder__")
    }

    val result = withStringCols
      .withColumn(ROW_LEVEL_PASS, conditionColumn("true"))
      .withColumn(ROW_LEVEL_FAIL, conditionColumn("false"))
      .withColumn(ROW_LEVEL_SKIP, {
        val allRuleNames = array(allRulesMap.values.toSeq.map(lit): _*)
        val passFailNames = array(booleanColumnNames.map(uuid => lit(uuidToRule.getOrElse(uuid, uuid))): _*)
        val nullNames = booleanColumnNames.map { uuid =>
          when(col(uuid).isNull, lit(uuidToRule.getOrElse(uuid, uuid))).otherwise(null)
        }
        val skipNames = array_except(allRuleNames, passFailNames)
        array_except(array_union(skipNames, array(nullNames: _*)), array(lit(null)))
      })
      .withColumn(ROW_LEVEL_OUTCOME_COLUMN,
        when(size(col(ROW_LEVEL_FAIL)).equalTo(0), Passed.asString).otherwise(Failed.asString))
      .drop(booleanColumnNames: _*)

    result
  }

  private def mapColumnsToRules(data: DataFrame, ruleOutcomes: Seq[RuleOutcome]): Map[String, DQRule] = {
    val columns = data.columns.toSet
    ruleOutcomes.flatMap { r =>
      r.rowLevelOutcome match {
        case _: NoColumn => None
        case s: SingularColumn => Some(s.columnName -> r.rule)
        case c: CompositeOutcome => Some(c.columnName -> r.rule)
      }
    }.filter { case (colName, _) => columns.contains(colName) }.toMap
  }

  private def composeRuleOutcomes(frame: DataFrame, ruleOutcomes: Seq[RuleOutcome]): DataFrame = {
    val (result, intermediateColumns) = ruleOutcomes.foldLeft((frame, Seq.empty[String])) {
      case ((df, intermediates), ruleOutcome) =>
        val (newDf, _, newIntermediates) = aggregateOutcomes(df, ruleOutcome.rowLevelOutcome)
        (newDf, intermediates ++ newIntermediates)
    }

    val neededAsSingular = ruleOutcomes.flatMap(_.rowLevelOutcome match {
      case s: SingularColumn => Some(s.columnName)
      case _ => None
    })
    val toDrop = intermediateColumns.distinct.diff(neededAsSingular)
    result.drop(toDrop: _*)
  }

  private def aggregateOutcomes(
    frame: DataFrame,
    outcome: RowLevelOutcome
  ): (DataFrame, Option[String], Seq[String]) = {
    outcome match {
      case _: NoColumn => (frame, None, Seq())
      case s: SingularColumn => (frame, Some(s.columnName), Seq())
      case composite: CompositeOutcome =>
        val (updatedFrame, colsToOperate, colsOperated) =
          composite.components.foldLeft((frame, Seq.empty[String], Seq.empty[String])) {
            case ((df, toOp, operated), nested) =>
              val (newDf, optCol, newOperated) = aggregateOutcomes(df, nested)
              val newToOp = optCol.map(toOp :+ _).getOrElse(toOp)
              (newDf, newToOp, operated ++ newOperated)
          }

        if (colsToOperate.size != composite.components.size) {
          (updatedFrame, None, colsToOperate)
        } else {
          val expr: Column = if (composite.operator == DQRuleLogicalOperator.AND) {
            colsToOperate.foldLeft(lit(true))(_ && col(_))
          } else {
            colsToOperate.foldLeft(lit(false))(_ || col(_))
          }
          (updatedFrame.withColumn(composite.columnName, expr),
            Some(composite.columnName),
            colsToOperate ++ colsOperated)
        }
    }
  }
}
