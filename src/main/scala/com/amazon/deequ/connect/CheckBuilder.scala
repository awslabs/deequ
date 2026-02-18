/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.deequ.connect

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.connect.proto.{CheckMessage, ConstraintMessage, PredicateMessage}

import scala.collection.JavaConverters._
import scala.util.matching.Regex

/**
 * Builds Deequ Check objects from protobuf messages.
 */
object CheckBuilder {

  /**
   * Build a Check from a CheckMessage protobuf.
   */
  def build(msg: CheckMessage): Check = {
    val level = msg.getLevel match {
      case CheckMessage.Level.ERROR => CheckLevel.Error
      case CheckMessage.Level.WARNING => CheckLevel.Warning
      case _ => CheckLevel.Error
    }

    var check = Check(level, msg.getDescription)

    msg.getConstraintsList.asScala.foreach { constraint =>
      check = addConstraint(check, constraint)
    }

    check
  }

  /**
   * Add a constraint to a Check based on the ConstraintMessage.
   */
  private def addConstraint(check: Check, c: ConstraintMessage): Check = {
    val hint = if (c.getHint.isEmpty) None else Some(c.getHint)
    val where = if (c.getWhere.isEmpty) None else Some(c.getWhere)

    c.getType match {
      // Completeness constraints
      case "isComplete" =>
        applyFilter(check.isComplete(c.getColumn, hint), where)

      case "hasCompleteness" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasCompleteness(c.getColumn, assertion, hint), where)

      case "areComplete" =>
        val columns = c.getColumnsList.asScala.toSeq
        applyFilter(check.areComplete(columns, hint), where)

      case "haveCompleteness" =>
        val columns = c.getColumnsList.asScala.toSeq
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.haveCompleteness(columns, assertion, hint), where)

      // Size constraint
      case "hasSize" =>
        val assertion = buildLongAssertion(c.getAssertion)
        applyFilter(check.hasSize(assertion, hint), where)

      // Uniqueness constraints
      case "isUnique" =>
        applyFilter(check.isUnique(c.getColumn, hint), where)

      case "hasUniqueness" =>
        val columns = if (c.getColumnsList.isEmpty) {
          Seq(c.getColumn)
        } else {
          c.getColumnsList.asScala.toSeq
        }
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasUniqueness(columns, assertion, hint), where)

      case "areUnique" =>
        val columns = c.getColumnsList.asScala.toSeq
        applyFilter(check.areUnique(columns, hint), where)

      case "isPrimaryKey" =>
        val columns = c.getColumnsList.asScala.toSeq
        if (columns.isEmpty) {
          applyFilter(check.isPrimaryKey(c.getColumn), where)
        } else {
          applyFilter(check.isPrimaryKey(c.getColumn, columns: _*), where)
        }

      // Distinctness
      case "hasDistinctness" =>
        val columns = c.getColumnsList.asScala.toSeq
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasDistinctness(columns, assertion, hint), where)

      // Statistical constraints
      case "hasMin" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasMin(c.getColumn, assertion, hint), where)

      case "hasMax" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasMax(c.getColumn, assertion, hint), where)

      case "hasMean" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasMean(c.getColumn, assertion, hint), where)

      case "hasSum" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasSum(c.getColumn, assertion, hint), where)

      case "hasStandardDeviation" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasStandardDeviation(c.getColumn, assertion, hint), where)

      // Length constraints
      case "hasMinLength" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasMinLength(c.getColumn, assertion, hint), where)

      case "hasMaxLength" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasMaxLength(c.getColumn, assertion, hint), where)

      // Quantile
      case "hasApproxQuantile" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasApproxQuantile(c.getColumn, c.getQuantile, assertion, hint), where)

      // Entropy
      case "hasEntropy" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasEntropy(c.getColumn, assertion, hint), where)

      // Correlation
      case "hasCorrelation" =>
        val columns = c.getColumnsList.asScala.toSeq
        val assertion = buildDoubleAssertion(c.getAssertion)
        if (columns.size >= 2) {
          applyFilter(check.hasCorrelation(columns(0), columns(1), assertion, hint), where)
        } else {
          check
        }

      // Pattern constraints
      case "hasPattern" =>
        val pattern = new Regex(c.getPattern)
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasPattern(c.getColumn, pattern, assertion, hint = hint), where)

      case "containsCreditCardNumber" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.containsCreditCardNumber(c.getColumn, assertion, hint), where)

      case "containsEmail" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.containsEmail(c.getColumn, assertion, hint), where)

      case "containsURL" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.containsURL(c.getColumn, assertion, hint), where)

      case "containsSocialSecurityNumber" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.containsSocialSecurityNumber(c.getColumn, assertion, hint), where)

      // Conditional constraints
      case "satisfies" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        val name = if (c.getConstraintName.isEmpty) "satisfies" else c.getConstraintName
        applyFilter(check.satisfies(c.getColumnCondition, name, assertion, hint), where)

      // Contained in
      case "isContainedIn" =>
        val allowedValues = c.getAllowedValuesList.asScala.toArray
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.isContainedIn(c.getColumn, allowedValues, assertion, hint), where)

      // Non-negative / positive
      case "isNonNegative" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.isNonNegative(c.getColumn, assertion, hint), where)

      case "isPositive" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.isPositive(c.getColumn, assertion, hint), where)

      // Comparison constraints
      case "isLessThan" =>
        val columns = c.getColumnsList.asScala.toSeq
        val assertion = buildDoubleAssertion(c.getAssertion)
        if (columns.size >= 2) {
          applyFilter(check.isLessThan(columns(0), columns(1), assertion, hint), where)
        } else {
          check
        }

      case "isLessThanOrEqualTo" =>
        val columns = c.getColumnsList.asScala.toSeq
        val assertion = buildDoubleAssertion(c.getAssertion)
        if (columns.size >= 2) {
          applyFilter(check.isLessThanOrEqualTo(columns(0), columns(1), assertion, hint), where)
        } else {
          check
        }

      case "isGreaterThan" =>
        val columns = c.getColumnsList.asScala.toSeq
        val assertion = buildDoubleAssertion(c.getAssertion)
        if (columns.size >= 2) {
          applyFilter(check.isGreaterThan(columns(0), columns(1), assertion, hint), where)
        } else {
          check
        }

      case "isGreaterThanOrEqualTo" =>
        val columns = c.getColumnsList.asScala.toSeq
        val assertion = buildDoubleAssertion(c.getAssertion)
        if (columns.size >= 2) {
          applyFilter(check.isGreaterThanOrEqualTo(columns(0), columns(1), assertion, hint), where)
        } else {
          check
        }

      // Approx count distinct
      case "hasApproxCountDistinct" =>
        val assertion = buildDoubleAssertion(c.getAssertion)
        applyFilter(check.hasApproxCountDistinct(c.getColumn, assertion, hint), where)

      // Unknown constraint type
      case unknownType =>
        throw new IllegalArgumentException(s"Unknown constraint type: $unknownType")
    }
  }

  /**
   * Apply a filter (where clause) to a check if specified.
   */
  private def applyFilter(
      checkWithConstraint: com.amazon.deequ.checks.CheckWithLastConstraintFilterable,
      where: Option[String]): Check = {
    where match {
      case Some(filter) => checkWithConstraint.where(filter)
      case None => checkWithConstraint
    }
  }

  /**
   * Build a Double => Boolean assertion function from PredicateMessage.
   */
  private def buildDoubleAssertion(pred: PredicateMessage): Double => Boolean = {
    if (pred == null) {
      // No predicate specified - default to 100% compliance
      (x: Double) => x == 1.0
    } else {
      pred.getOperator match {
        case PredicateMessage.Operator.EQ => (x: Double) => x == pred.getValue
        case PredicateMessage.Operator.NE => (x: Double) => x != pred.getValue
        case PredicateMessage.Operator.GT => (x: Double) => x > pred.getValue
        case PredicateMessage.Operator.GE => (x: Double) => x >= pred.getValue
        case PredicateMessage.Operator.LT => (x: Double) => x < pred.getValue
        case PredicateMessage.Operator.LE => (x: Double) => x <= pred.getValue
        case PredicateMessage.Operator.BETWEEN =>
          (x: Double) => x >= pred.getLowerBound && x <= pred.getUpperBound
        case PredicateMessage.Operator.UNSPECIFIED | _ =>
          // UNSPECIFIED or unknown operator - default to 100% compliance
          (x: Double) => x == 1.0
      }
    }
  }

  /**
   * Build a Long => Boolean assertion function from PredicateMessage.
   */
  private def buildLongAssertion(pred: PredicateMessage): Long => Boolean = {
    if (pred == null) {
      (_: Long) => true
    } else {
      pred.getOperator match {
        case PredicateMessage.Operator.EQ => (x: Long) => x == pred.getValue.toLong
        case PredicateMessage.Operator.NE => (x: Long) => x != pred.getValue.toLong
        case PredicateMessage.Operator.GT => (x: Long) => x > pred.getValue.toLong
        case PredicateMessage.Operator.GE => (x: Long) => x >= pred.getValue.toLong
        case PredicateMessage.Operator.LT => (x: Long) => x < pred.getValue.toLong
        case PredicateMessage.Operator.LE => (x: Long) => x <= pred.getValue.toLong
        case PredicateMessage.Operator.BETWEEN =>
          (x: Long) => x >= pred.getLowerBound.toLong && x <= pred.getUpperBound.toLong
        case PredicateMessage.Operator.UNSPECIFIED | _ => (_: Long) => true
      }
    }
  }
}
