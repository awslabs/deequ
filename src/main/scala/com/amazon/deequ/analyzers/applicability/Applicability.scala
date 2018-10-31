/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
  *
  */

package com.amazon.deequ.analyzers.applicability

import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.{AnalysisBasedConstraint, Constraint, ConstraintDecorator}
import com.amazon.deequ.metrics.Metric
import java.sql.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.Failure

private[deequ] case class ApplicabilityResult(
  isApplicable: Boolean,
  failures: Seq[(String, Exception)]
)

private[deequ] object Applicability {

  private def shouldBeNull(nullable: Boolean): Boolean = {
    nullable && math.random < 0.01
  }

  def randomBoolean(nullable: Boolean): java.lang.Boolean = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      util.Random.nextDouble() > 0.5
    }
  }

  def randomInteger(nullable: Boolean): java.lang.Integer = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      util.Random.nextInt()
    }
  }

  def randomFloat(nullable: Boolean): java.lang.Float = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      util.Random.nextFloat()
    }
  }

  def randomDouble(nullable: Boolean): java.lang.Double = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      util.Random.nextDouble()
    }
  }

  def randomByte(nullable: Boolean): java.lang.Byte = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      util.Random.nextInt().toByte
    }
  }

  def randomShort(nullable: Boolean): java.lang.Short = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      util.Random.nextInt().toShort
    }
  }

  def randomLong(nullable: Boolean): java.lang.Long = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      util.Random.nextLong()
    }
  }

  def randomDecimal(nullable: Boolean): java.math.BigDecimal = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      BigDecimal(util.Random.nextLong()).bigDecimal
    }
  }

  def randomTimestamp(nullable: Boolean): java.sql.Timestamp = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      new Timestamp(util.Random.nextLong())
    }
  }

  def randomString(nullable: Boolean): java.lang.String = {
    if (shouldBeNull(nullable)) {
      null
    } else {
      val length = util.Random.nextInt(20) + 1
      util.Random.alphanumeric.take(length).mkString
    }
  }
}

/**
  * A class to Check whether a check is applicable to some data using the schema of the data.
  *
  * @param session The spark session in order to be able to create fake data
  */
private[deequ] class Applicability(session: SparkSession) {

  import Applicability._

  /**
    * Check whether a check is applicable to some data using the schema of the data.
    *
    * @param check A check that may be applicable to some data
    * @param schema The schema of the data the checks are for
    */
  def isApplicable(check: Check, schema: StructType): ApplicabilityResult = {

    val data = generateRandomData(schema, 1000)

    val constraintsByName = check.constraints
      .map { constraint => constraint.toString -> constraint }
      .map {
        case (name, nc: ConstraintDecorator) => name -> nc.inner
        case (name, c: Constraint) => name -> c
      }
      .collect { case (name, constraint: AnalysisBasedConstraint[_, _, _]) =>
        (name, constraint)
      }


    val failures = constraintsByName
      .flatMap { case (name, constraint) =>
        val maybeValue = constraint.analyzer.calculate(data).value

        maybeValue match {
          // An exception occurred during analysis
          case Failure(exception: Exception) => Some(name -> exception)
          // Analysis done successfully and result metric is there
          case _ => None
        }
      }

    ApplicabilityResult(failures.isEmpty, failures)
  }

  /**
    * Check whether analyzers are applicable to some data using the schema of the data.
    *
    * @param analyzers Analyzers that may be applicable to some data
    * @param schema The schema of the data the analyzers are for
    */
  def isApplicable(analyzers: Seq[Analyzer[_ <: State[_], Metric[_]]], schema: StructType)
    : ApplicabilityResult = {

    val data = generateRandomData(schema, 1000)

    val analyzersByName = analyzers
      .map { analyzer => analyzer.toString -> analyzer }

    val failures = analyzersByName
      .flatMap { case (name, analyzer) =>
        val maybeValue = analyzer.calculate(data).value

        maybeValue match {
          // An exception occurred during analysis
          case Failure(exception) => Some(name -> exception)
          // Analysis done successfully and result metric is there
          case _ => None
        }
      }

    ApplicabilityResult(failures.isEmpty, failures)
  }


  def generateRandomData(schema: StructType, numRows: Int): DataFrame = {

    val rows = (0 to numRows).map { _ =>

      val cells = schema.fields.map { field =>

        val cell: scala.Any = field.dataType match {
          case StringType => randomString(field.nullable)
          case IntegerType => randomInteger(field.nullable)
          case FloatType => randomFloat(field.nullable)
          case DoubleType => randomDouble(field.nullable)
          case ByteType => randomByte(field.nullable)
          case ShortType => randomShort(field.nullable)
          case LongType => randomLong(field.nullable)
          case _ : DecimalType => randomDecimal(field.nullable)
          case TimestampType => randomTimestamp(field.nullable)
          case BooleanType => randomBoolean(field.nullable)
          case _ =>
            throw new IllegalArgumentException(
              "Applicability check can only handle basic datatypes " +
                s"for columns (string, integer, float, double, decimal, boolean) " +
                s"not ${field.dataType}")
        }

        cell
      }

      Row(cells: _*)
    }

    session.createDataFrame(session.sparkContext.parallelize(rows), schema)
  }
}
