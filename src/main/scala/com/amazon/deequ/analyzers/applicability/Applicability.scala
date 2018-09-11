package com.amazon.deequ.analyzers.applicability

import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.{AnalysisBasedConstraint, Constraint, ConstraintDecorator}
import com.amazon.deequ.metrics.Metric
import java.sql.Timestamp
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.Failure

private[deequ] case class ApplicabilityResult(
  isApplicable: Boolean,
  failures: Seq[(String, Throwable)]
)

/**
  * A class to Check whether a check is applicable to some data using the schema of the data.
  *
  * @param session The spark session in order to be able to create fake data
  */
private[deequ] class Applicability(session: SparkSession) {


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
          case Failure(exception) => Some(name -> exception)
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


  def randomString(nullable: Boolean = false): java.lang.String = {
    if (nullable && math.random < 0.01) {
      null
    } else {
      val length = (math.random * 20 + 1).toInt
      RandomStringUtils.randomAlphanumeric(length)
    }
  }

  def randomInteger(nullable: Boolean = false): java.lang.Integer = {
    if (nullable && math.random < 0.01) {
      null
    } else {
      (math.random * 100).toInt
    }
  }

  def randomFloat(nullable: Boolean = false): java.lang.Float = {
    if (nullable && math.random < 0.01) {
      null
    } else {
      (math.random * 100).toFloat
    }
  }


  def randomDouble(nullable: Boolean = false): java.lang.Double = {
    if (nullable && math.random < 0.01) {
      null
    } else {
      math.random * 100
    }
  }

  def randomDecimal(nullable: Boolean = false): java.math.BigDecimal = {
    if (nullable && math.random < 0.01) {
      null
    } else {
      BigDecimal(math.random * 100).bigDecimal
    }
  }

  def randomTimestamp(nullable: Boolean = false): java.sql.Timestamp = {
    if (nullable && math.random < 0.01) {
      null
    } else {
      new Timestamp((math.random * 100).toLong)
    }
  }

  def randomBoolean(nullable: Boolean = false): java.lang.Boolean = {
    if (nullable && math.random < 0.01) {
      null
    } else {
      math.random > 0.5
    }
  }
}
