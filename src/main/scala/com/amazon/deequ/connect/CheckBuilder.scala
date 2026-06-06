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

import com.amazon.deequ.checks.{Check, CheckLevel, CheckWithLastConstraintFilterable}
import com.amazon.deequ.connect.proto.{
  CheckLevel => ProtoCheckLevel,
  Check => ProtoCheck,
  Constraint => ProtoConstraint,
  Predicate => ProtoPredicate
}

import scala.collection.JavaConverters._
import scala.util.matching.Regex

/**
 * Builds Deequ Check objects from the Stage 1 protobuf shape.
 *
 * Each constraint is encoded as a `oneof` arm on `Constraint`, with one arm
 * per builder method on `Check`. We pattern-match on `getBodyCase` and the
 * Scala compiler enforces exhaustiveness - the previous string-typed
 * `case _ => throw` default is unreachable.
 *
 * See ADR-0001 (oneof arms per builder method) and the schema at
 * `src/main/protobuf/deequ_connect.proto`.
 */
object CheckBuilder {

  def build(msg: ProtoCheck): Check = {
    val level = msg.getLevel match {
      case ProtoCheckLevel.CHECK_LEVEL_ERROR => CheckLevel.Error
      case ProtoCheckLevel.CHECK_LEVEL_WARNING => CheckLevel.Warning
      case _ => CheckLevel.Error
    }

    msg.getConstraintsList.asScala.foldLeft(Check(level, msg.getDescription)) {
      (acc, constraint) => addConstraint(acc, constraint)
    }
  }

  private def addConstraint(check: Check, c: ProtoConstraint): Check = {
    val hint = optString(c.getHint)
    val where = optString(c.getWhere)

    import ProtoConstraint.BodyCase._
    c.getBodyCase match {

      // ---- Completeness -----------------------------------------------------
      case IS_COMPLETE =>
        applyFilter(check.isComplete(c.getIsComplete.getColumn, hint), where)

      case HAS_COMPLETENESS =>
        val s = c.getHasCompleteness
        applyFilter(check.hasCompleteness(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      case ARE_COMPLETE =>
        applyFilter(check.areComplete(c.getAreComplete.getColumnsList.asScala.toSeq, hint), where)

      case HAVE_COMPLETENESS =>
        val s = c.getHaveCompleteness
        applyFilter(
          check.haveCompleteness(s.getColumnsList.asScala.toSeq, doubleAssertion(s.getAssertion), hint),
          where)

      // ---- Size -------------------------------------------------------------
      case HAS_SIZE =>
        applyFilter(check.hasSize(longAssertion(c.getHasSize.getAssertion), hint), where)

      // ---- Uniqueness / distinctness ---------------------------------------
      case IS_UNIQUE =>
        applyFilter(check.isUnique(c.getIsUnique.getColumn, hint), where)

      case HAS_UNIQUENESS =>
        val s = c.getHasUniqueness
        applyFilter(
          check.hasUniqueness(s.getColumnsList.asScala.toSeq, doubleAssertion(s.getAssertion), hint),
          where)

      case ARE_UNIQUE =>
        applyFilter(check.areUnique(c.getAreUnique.getColumnsList.asScala.toSeq, hint), where)

      case IS_PRIMARY_KEY =>
        val s = c.getIsPrimaryKey
        val additional = s.getAdditionalColumnsList.asScala.toSeq
        if (additional.isEmpty) {
          applyFilter(check.isPrimaryKey(s.getColumn), where)
        } else {
          applyFilter(check.isPrimaryKey(s.getColumn, additional: _*), where)
        }

      case HAS_DISTINCTNESS =>
        val s = c.getHasDistinctness
        applyFilter(
          check.hasDistinctness(s.getColumnsList.asScala.toSeq, doubleAssertion(s.getAssertion), hint),
          where)

      case HAS_UNIQUE_VALUE_RATIO =>
        val s = c.getHasUniqueValueRatio
        applyFilter(
          check.hasUniqueValueRatio(s.getColumnsList.asScala.toSeq, doubleAssertion(s.getAssertion), hint),
          where)

      // ---- Statistical ------------------------------------------------------
      case HAS_MIN =>
        val s = c.getHasMin
        applyFilter(check.hasMin(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      case HAS_MAX =>
        val s = c.getHasMax
        applyFilter(check.hasMax(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      case HAS_MEAN =>
        val s = c.getHasMean
        applyFilter(check.hasMean(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      case HAS_SUM =>
        val s = c.getHasSum
        applyFilter(check.hasSum(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      case HAS_STANDARD_DEVIATION =>
        val s = c.getHasStandardDeviation
        applyFilter(check.hasStandardDeviation(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      // ---- Length -----------------------------------------------------------
      case HAS_MIN_LENGTH =>
        val s = c.getHasMinLength
        applyFilter(check.hasMinLength(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      case HAS_MAX_LENGTH =>
        val s = c.getHasMaxLength
        applyFilter(check.hasMaxLength(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      // ---- Quantile ---------------------------------------------------------
      case HAS_APPROX_QUANTILE =>
        val s = c.getHasApproxQuantile
        val q = if (s.hasQuantile) s.getQuantile else 0.5
        applyFilter(
          check.hasApproxQuantile(s.getColumn, q, doubleAssertion(s.getAssertion), hint),
          where)

      // ---- Information theory ----------------------------------------------
      case HAS_ENTROPY =>
        val s = c.getHasEntropy
        applyFilter(check.hasEntropy(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      // ---- Correlation ------------------------------------------------------
      case HAS_CORRELATION =>
        val s = c.getHasCorrelation
        applyFilter(
          check.hasCorrelation(s.getColumnA, s.getColumnB, doubleAssertion(s.getAssertion), hint),
          where)

      // ---- Pattern matching -------------------------------------------------
      case HAS_PATTERN =>
        val s = c.getHasPattern
        applyFilter(
          check.hasPattern(s.getColumn, new Regex(s.getRegex), doubleAssertion(s.getAssertion), hint = hint),
          where)

      case CONTAINS_CREDIT_CARD_NUMBER =>
        val s = c.getContainsCreditCardNumber
        applyFilter(
          check.containsCreditCardNumber(s.getColumn, doubleAssertion(s.getAssertion), hint),
          where)

      case CONTAINS_EMAIL =>
        val s = c.getContainsEmail
        applyFilter(check.containsEmail(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      case CONTAINS_URL =>
        val s = c.getContainsUrl
        applyFilter(check.containsURL(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      case CONTAINS_SOCIAL_SECURITY_NUMBER =>
        val s = c.getContainsSocialSecurityNumber
        applyFilter(
          check.containsSocialSecurityNumber(s.getColumn, doubleAssertion(s.getAssertion), hint),
          where)

      // ---- SQL condition ----------------------------------------------------
      case SATISFIES =>
        val s = c.getSatisfies
        val name = if (s.getConstraintName.isEmpty) "satisfies" else s.getConstraintName
        applyFilter(
          check.satisfies(s.getColumnCondition, name, doubleAssertion(s.getAssertion), hint),
          where)

      // ---- Membership -------------------------------------------------------
      case IS_CONTAINED_IN =>
        val s = c.getIsContainedIn
        val allowed = s.getAllowedValuesList.asScala.toArray
        applyFilter(
          check.isContainedIn(s.getColumn, allowed, doubleAssertion(s.getAssertion), hint),
          where)

      // ---- Sign -------------------------------------------------------------
      case IS_NON_NEGATIVE =>
        val s = c.getIsNonNegative
        applyFilter(check.isNonNegative(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      case IS_POSITIVE =>
        val s = c.getIsPositive
        applyFilter(check.isPositive(s.getColumn, doubleAssertion(s.getAssertion), hint), where)

      // ---- Inter-column comparison -----------------------------------------
      case IS_LESS_THAN =>
        val s = c.getIsLessThan
        applyFilter(
          check.isLessThan(s.getColumnA, s.getColumnB, doubleAssertion(s.getAssertion), hint),
          where)

      case IS_LESS_THAN_OR_EQUAL_TO =>
        val s = c.getIsLessThanOrEqualTo
        applyFilter(
          check.isLessThanOrEqualTo(s.getColumnA, s.getColumnB, doubleAssertion(s.getAssertion), hint),
          where)

      case IS_GREATER_THAN =>
        val s = c.getIsGreaterThan
        applyFilter(
          check.isGreaterThan(s.getColumnA, s.getColumnB, doubleAssertion(s.getAssertion), hint),
          where)

      case IS_GREATER_THAN_OR_EQUAL_TO =>
        val s = c.getIsGreaterThanOrEqualTo
        applyFilter(
          check.isGreaterThanOrEqualTo(s.getColumnA, s.getColumnB, doubleAssertion(s.getAssertion), hint),
          where)

      // ---- Cardinality estimation ------------------------------------------
      case HAS_APPROX_COUNT_DISTINCT =>
        val s = c.getHasApproxCountDistinct
        applyFilter(
          check.hasApproxCountDistinct(s.getColumn, doubleAssertion(s.getAssertion), hint),
          where)

      case BODY_NOT_SET =>
        throw new IllegalArgumentException(
          "Constraint has no body set; client must populate one of the oneof arms")
    }
  }

  private def optString(s: String): Option[String] =
    if (s == null || s.isEmpty) None else Some(s)

  private def applyFilter(
      checkWithConstraint: CheckWithLastConstraintFilterable,
      where: Option[String]): Check = where match {
    case Some(filter) => checkWithConstraint.where(filter)
    case None => checkWithConstraint
  }

  /** `x => Boolean`-style predicate for Double metrics. */
  private def doubleAssertion(pred: ProtoPredicate): Double => Boolean = {
    if (pred == null) {
      // Schema enforces a Predicate on every arm that declares one. A null
      // here means the client violated the contract - fail loud.
      throw new IllegalArgumentException("Predicate is missing on a constraint that requires it")
    }
    pred.getOp match {
      case ProtoPredicate.CompareOp.COMPARE_OP_EQ => (x: Double) => x == pred.getValue
      case ProtoPredicate.CompareOp.COMPARE_OP_NE => (x: Double) => x != pred.getValue
      case ProtoPredicate.CompareOp.COMPARE_OP_GT => (x: Double) => x > pred.getValue
      case ProtoPredicate.CompareOp.COMPARE_OP_GE => (x: Double) => x >= pred.getValue
      case ProtoPredicate.CompareOp.COMPARE_OP_LT => (x: Double) => x < pred.getValue
      case ProtoPredicate.CompareOp.COMPARE_OP_LE => (x: Double) => x <= pred.getValue
      case ProtoPredicate.CompareOp.COMPARE_OP_BETWEEN =>
        (x: Double) => x >= pred.getLowerBound && x <= pred.getUpperBound
      case op =>
        throw new IllegalArgumentException(s"Unsupported predicate operator: $op")
    }
  }

  private def longAssertion(pred: ProtoPredicate): Long => Boolean = {
    if (pred == null) {
      throw new IllegalArgumentException("Predicate is missing on hasSize constraint")
    }
    pred.getOp match {
      case ProtoPredicate.CompareOp.COMPARE_OP_EQ => (x: Long) => x == pred.getValue.toLong
      case ProtoPredicate.CompareOp.COMPARE_OP_NE => (x: Long) => x != pred.getValue.toLong
      case ProtoPredicate.CompareOp.COMPARE_OP_GT => (x: Long) => x > pred.getValue.toLong
      case ProtoPredicate.CompareOp.COMPARE_OP_GE => (x: Long) => x >= pred.getValue.toLong
      case ProtoPredicate.CompareOp.COMPARE_OP_LT => (x: Long) => x < pred.getValue.toLong
      case ProtoPredicate.CompareOp.COMPARE_OP_LE => (x: Long) => x <= pred.getValue.toLong
      case ProtoPredicate.CompareOp.COMPARE_OP_BETWEEN =>
        (x: Long) => x >= pred.getLowerBound.toLong && x <= pred.getUpperBound.toLong
      case op =>
        throw new IllegalArgumentException(s"Unsupported predicate operator: $op")
    }
  }
}
