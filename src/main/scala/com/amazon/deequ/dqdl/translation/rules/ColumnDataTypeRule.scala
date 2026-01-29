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

import com.amazon.deequ.analyzers.FilteredRowOutcome.FilteredRowOutcome
import com.amazon.deequ.dqdl.execution.DefaultOperandEvaluator
import com.amazon.deequ.dqdl.model.ColumnDataTypeExecutableRule
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat, lit, to_date, when}
import org.apache.spark.sql.types._
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.NumberBasedCondition
import software.amazon.glue.dqdl.model.condition.string.{StringBasedCondition, StringBasedConditionOperator}

import scala.jdk.CollectionConverters._

object ColumnDataTypeRule {

  private val DecimalPattern = """DECIMAL\((\d+),\s*(\d+)\)""".r

  private val ValidFullDateFormats = Set(
    "dd-MM-yyyy", "MM-dd-yyyy", "yyyy-MM-dd",
    "dd/MM/yyyy", "MM/dd/yyyy", "yyyy/MM/dd"
  )

  private val ValidPartialDateFormats = Set(
    "MM-yyyy", "dd-MM", "MM-dd", "yyyy-MM",
    "MM/yyyy", "dd/MM", "MM/dd", "yyyy/MM"
  )

  def toExecutableRule(rule: DQRule, filteredRowOutcome: FilteredRowOutcome):
      Either[String, ColumnDataTypeExecutableRule] = {
    val columnName = rule.getParameters.asScala("TargetColumn")
    val tags = Option(rule.getTags).map(_.asScala.toMap).getOrElse(Map.empty[String, String])

    rule.getCondition match {
      case condition: StringBasedCondition =>
        condition.getOperator match {
          case StringBasedConditionOperator.EQUALS | StringBasedConditionOperator.NOT_EQUALS =>
            val isEquals = condition.getOperator == StringBasedConditionOperator.EQUALS
            val operands = condition.getOperands.asScala
            if (operands.size != 1) {
              Left("ColumnDataType rule requires exactly one data type operand")
            } else {
              val dataTypeStr = operands.head.getOperand
              dataTypeToSparkType(dataTypeStr) match {
                case Some(sparkType) =>
                  if (isUnsupportedTagsFormat(sparkType, tags)) {
                    Left("Unsupported rule format")
                  } else {
                    val castSuccessful = castColumnToSparkType(columnName, sparkType, tags)
                    val outcomeColumn = when(castSuccessful, lit(isEquals)).otherwise(lit(!isEquals))
                    val assertion = getAssertion(rule)
                    Right(ColumnDataTypeExecutableRule(rule, columnName, outcomeColumn, assertion, filteredRowOutcome))
                  }
                case None =>
                  Left(s"Unrecognized data type: $dataTypeStr")
              }
            }
          case StringBasedConditionOperator.MATCHES | StringBasedConditionOperator.NOT_MATCHES |
               StringBasedConditionOperator.IN | StringBasedConditionOperator.NOT_IN =>
            Left("Unsupported operator")
          case _ =>
            Left("ColumnDataType rule only supports EQUALS (=) or NOT_EQUALS (!=) operators")
        }
      case _ =>
        Left("ColumnDataType rule requires a string-based condition")
    }
  }

  private def dataTypeToSparkType(dataType: String): Option[DataType] = {
    dataType.toUpperCase match {
      case "BOOLEAN" => Some(BooleanType)
      case "DATE" => Some(DateType)
      case "TIMESTAMP" => Some(TimestampType)
      case "INTEGER" => Some(IntegerType)
      case "DOUBLE" => Some(DoubleType)
      case "FLOAT" => Some(FloatType)
      case "LONG" => Some(LongType)
      case DecimalPattern(precision, scale) =>
        Some(DecimalType(precision.toInt, scale.toInt))
      case _ => None
    }
  }

  private def isUnsupportedTagsFormat(sparkType: DataType, tags: Map[String, String]): Boolean = {
    sparkType == DateType && tags.nonEmpty && !tags.contains("format")
  }

  private def castColumnToSparkType(targetColumn: String, sparkType: DataType,
                                    tags: Map[String, String]): Column = {
    sparkType match {
      case DateType =>
        if (isUnsupportedTagsFormat(sparkType, tags)) {
          lit(false)
        } else {
          tags.get("format") match {
            case Some(fmt) if fmt.nonEmpty =>
              if (validateDateFormat(fmt)) {
                if (validatePartialDateFormat(fmt)) {
                  val (exprColumn, outputFormat) = handlePartialDates(targetColumn, fmt)
                  to_date(exprColumn, outputFormat).cast(sparkType).isNotNull
                } else {
                  val colExpr = col(targetColumn).cast(StringType)
                  to_date(colExpr, fmt).cast(sparkType).isNotNull
                }
              } else {
                lit(false)
              }
            case _ =>
              col(targetColumn).cast(sparkType).isNotNull
          }
        }
      case _ =>
        col(targetColumn).cast(sparkType).isNotNull
    }
  }

  private def validateDateFormat(fmt: String): Boolean = {
    ValidFullDateFormats.contains(fmt) || ValidPartialDateFormats.contains(fmt)
  }

  private def validatePartialDateFormat(fmt: String): Boolean = {
    ValidPartialDateFormats.contains(fmt)
  }

  private def handlePartialDates(targetColumn: String, inputFormat: String): (Column, String) = {
    inputFormat match {
      case "MM-yyyy" =>
        (concat(lit("01-"), col(targetColumn)), "dd-MM-yyyy")
      case "yyyy-MM" =>
        (concat(col(targetColumn), lit("-01")), "yyyy-MM-dd")
      case "MM/yyyy" =>
        (concat(lit("01/"), col(targetColumn)), "dd/MM/yyyy")
      case "yyyy/MM" =>
        (concat(col(targetColumn), lit("/01")), "yyyy/MM/dd")
      case "dd-MM" =>
        (concat(col(targetColumn), lit("-2000")), "dd-MM-yyyy")
      case "dd/MM" =>
        (concat(col(targetColumn), lit("/2000")), "dd/MM/yyyy")
      case "MM/dd" =>
        (concat(col(targetColumn), lit("/2000")), "MM/dd/yyyy")
      case "MM-dd" =>
        (concat(col(targetColumn), lit("-2000")), "MM-dd-yyyy")
      case _ =>
        (col(targetColumn).cast(StringType), inputFormat)
    }
  }

  private def getAssertion(rule: DQRule): Double => Boolean = {
    Option(rule.getThresholdCondition) match {
      case Some(threshold: NumberBasedCondition) if threshold.getConditionAsString.nonEmpty =>
        (d: Double) => threshold.evaluate(d, rule, DefaultOperandEvaluator)
      case _ =>
        (d: Double) => d == 1.0
    }
  }
}
