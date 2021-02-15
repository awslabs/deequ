/** Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.schema

import org.apache.spark.sql.functions.{
  col,
  lit,
  expr,
  length,
  not,
  unix_timestamp,
  regexp_extract,
  when,
  concat,
  concat_ws
}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types.{
  DataTypes,
  DecimalType,
  IntegerType,
  StringType,
  TimestampType,
  StructField,
  StructType,
  FloatType
}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import com.amazon.deequ.analyzers.{Patterns}
import scala.util.matching.Regex

import org.apache.spark.sql.SparkSession

sealed trait ColumnDefinition {
  def name: String
  def isNullable: Boolean
  def description: String
  def castExpression(): Column = { col(name) }
}

case class StringColumnDefinition(
    name: String,
    isNullable: Boolean = true,
    minLength: Option[Int] = None,
    maxLength: Option[Int] = None,
    matches: Option[String] = None,
    description: String
) extends ColumnDefinition {}

case class IntColumnDefinition(
    name: String,
    isNullable: Boolean = true,
    minValue: Option[Int] = None,
    maxValue: Option[Int] = None,
    description: String
) extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(IntegerType).as(name)
  }
}

case class DecimalColumnDefinition(
    name: String,
    precision: Int,
    scale: Int,
    isNullable: Boolean = true,
    description: String
) extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(DecimalType(precision, scale)).as(name)
  }
}

case class TimestampColumnDefinition(
    name: String,
    mask: String,
    isNullable: Boolean = true,
    description: String
) extends ColumnDefinition {

  override def castExpression(): Column = {
    unix_timestamp(col(name), mask).cast(TimestampType).as(name)
  }
}

sealed trait CheckDefinition {
  def name: String
  def isNullable: Boolean
  def description: String
}

case class PatternDefinition(
    name: String,
    isNullable: Boolean,
    pattern: Regex,
    description: String
) extends ColumnDefinition

case class NonNegativeDefinition(
    name: String,
    isNullable: Boolean,
    description: String
) extends ColumnDefinition {}

case class ColumnGreaterThanDefinition(
    name: String,
    columnB: String,
    canEqual: Boolean,
    isNullable: Boolean,
    description: String
) extends ColumnDefinition {}

case class GreaterThanDefinition(
    name: String,
    minValue: Int,
    canEqual: Boolean,
    isNullable: Boolean,
    description: String
) extends ColumnDefinition {}

case class ColumnLessThanDefinition(
    name: String,
    columnB: String,
    canEqual: Boolean,
    isNullable: Boolean,
    description: String
) extends ColumnDefinition {}

case class LessThanDefinition(
    name: String,
    maxValue: Int,
    canEqual: Boolean,
    isNullable: Boolean,
    description: String
) extends ColumnDefinition {}

case class RangeDefinition(
    name: String,
    minValue: Int,
    maxValue: Int,
    inclusive: Boolean,
    isNullable: Boolean,
    description: String
) extends ColumnDefinition {}

case class ContainedInDefinition(
    name: String,
    allowedValues: Array[String],
    isNullable: Boolean,
    description: String
) extends ColumnDefinition {}

/** A simple schema definition for relational data in Andes */
case class RowLevelSchema(
    columnDefinitions: Seq[ColumnDefinition] = Seq.empty
) {

  /** Declare a textual column
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @param minLength  minimum length of values
    * @param maxLength  maximum length of values
    * @param matches regular expression which the column value must match
    * @return
    */
  def withStringColumn(
      name: String,
      isNullable: Boolean = true,
      minLength: Option[Int] = None,
      maxLength: Option[Int] = None,
      matches: Option[String] = None
  ): RowLevelSchema = {
    val description = "StringSchema"
    RowLevelSchema(
      columnDefinitions :+ StringColumnDefinition(
        name,
        isNullable,
        minLength,
        maxLength,
        matches,
        description
      )
    )
  }

  /** Declare an integer column
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @param minValue minimum value
    * @param maxValue maximum value
    * @return
    */
  def withIntColumn(
      name: String,
      isNullable: Boolean = true,
      minValue: Option[Int] = None,
      maxValue: Option[Int] = None
  ): RowLevelSchema = {
    val description = "IntegerSchema"
    RowLevelSchema(
      columnDefinitions :+ IntColumnDefinition(
        name,
        isNullable,
        minValue,
        maxValue,
        description
      )
    )
  }

  /** Declare a decimal column
    *
    * @param name column name
    * @param precision  precision of values
    * @param scale  scale of values
    * @param isNullable are NULL values permitted?
    * @return
    */
  def withDecimalColumn(
      name: String,
      precision: Int,
      scale: Int,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "DecimalSchema"
    RowLevelSchema(
      columnDefinitions :+ DecimalColumnDefinition(
        name,
        precision,
        scale,
        isNullable,
        description
      )
    )
  }

  /** Declare a timestamp column
    *
    * @param name column name
    * @param mask pattern for the timestamp
    * @param isNullable are NULL values permitted?
    * @return
    */
  def withTimestampColumn(
      name: String,
      mask: String,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "TimestampSchema"
    RowLevelSchema(
      columnDefinitions :+ TimestampColumnDefinition(
        name,
        mask,
        isNullable,
        description
      )
    )
  }

  /** Checks if String contains email pattern
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
  def containsEmail(
      name: String,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "ContainsEmailCheck"
    RowLevelSchema(
      columnDefinitions :+ PatternDefinition(
        name,
        isNullable,
        Patterns.EMAIL,
        description
      )
    )
  }

  /** Check if string contains social security number
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
  def containsSocialSecurityNumber(
      name: String,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "ContainsSSNCheck"
    RowLevelSchema(
      columnDefinitions :+ PatternDefinition(
        name,
        isNullable,
        Patterns.SOCIAL_SECURITY_NUMBER_US,
        description
      )
    )
  }

  /** Check if string contains URL pattern
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
  def containsURL(name: String, isNullable: Boolean = true): RowLevelSchema = {
    val description = "ContainsURLCheck"
    RowLevelSchema(
      columnDefinitions :+ PatternDefinition(
        name,
        isNullable,
        Patterns.URL,
        description
      )
    )
  }

  /** Check if string contains credit card number
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
  def containsCreditCardNumber(
      name: String,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "ContainsCreditCardNumberCheck"
    RowLevelSchema(
      columnDefinitions :+ PatternDefinition(
        name,
        isNullable,
        Patterns.CREDITCARD,
        description
      )
    )
  }

  /** Check if string is contained in a set array of string values
    *
    * @param name column name
    * @param allowedValues array of values to check if string contained in
    * @param isNullable are NULL values permitted?
    * @return
    */
  def isContainedIn(
      name: String,
      allowedValues: Array[String],
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "isContainedInCheck"
    RowLevelSchema(
      columnDefinitions :+ ContainedInDefinition(
        name,
        allowedValues,
        isNullable,
        description
      )
    )
  }

  /** Check if int column is nonnegative
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
  def isNonNegative(
      name: String,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "isNonNegativeCheck"
    RowLevelSchema(
      columnDefinitions :+ NonNegativeDefinition(name, isNullable, description)
    )
  }

  /** Check if int column is greater than a minimum value
    *
    * @param name column name
    * @param minValue minimum value integer must be larger than
    * @param isNullable are NULL values permitted?
    * @return
    */
  def isGreaterThan(
      name: String,
      minValue: Int,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "isGreaterThanCheck"
    val canEqual = false
    RowLevelSchema(
      columnDefinitions :+ GreaterThanDefinition(
        name,
        minValue,
        canEqual,
        isNullable,
        description
      )
    )
  }

  /** Check if int column is greater than or equal to a minimum value
    *
    * @param name column name
    * @param minValue minimum value that row value must be larger than or equal to
    * @param isNullable are NULL values permitted?
    * @return
    */
  def isGreaterThanOrEqualTo(
      name: String,
      minValue: Int,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "isGreaterThanorEqualToCheck"
    val canEqual = false
    RowLevelSchema(
      columnDefinitions :+ GreaterThanDefinition(
        name,
        minValue,
        canEqual,
        isNullable,
        description
      )
    )
  }

  /** Check if int column is less than a maximum value
    *
    * @param name column name
    * @param maxValue maximum value that row value must be less than
    * @param isNullable are NULL values permitted?
    * @return
    */
  def isLessThan(
      name: String,
      maxValue: Int,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "isLessThanCheck"
    val canEqual = false
    RowLevelSchema(
      columnDefinitions :+ LessThanDefinition(
        name,
        maxValue,
        canEqual,
        isNullable,
        description
      )
    )
  }

  /** Check if int column is less than or equal to a maximum value
    *
    * @param name column name
    * @param maxValue maximum value that row value must be less than or equal to
    * @param isNullable are NULL values permitted?
    * @return
    */
  def isLessThanOrEqualTo(
      name: String,
      maxValue: Int,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "isLessThanOrEqualToCheck"
    val canEqual = false
    RowLevelSchema(
      columnDefinitions :+ LessThanDefinition(
        name,
        maxValue,
        canEqual,
        isNullable,
        description
      )
    )
  }

  /** Check if int column is in a range of values
    *
    * @param name column name
    * @param minValue lower bound of the range that row value must be greater than
    * @param maxValue upper bound of the range integer must be less than
    * @param inclusive is the range inclusive of the lower and upper bound?
    * @param isNullable are NULL values permitted?
    * @return
    */
  def isInRange(
      name: String,
      minValue: Int,
      maxValue: Int,
      inclusive: Boolean = true,
      isNullable: Boolean = true
  ): RowLevelSchema = {
    val description = "isInRangeCheck"
    RowLevelSchema(
      columnDefinitions :+ RangeDefinition(
        name,
        minValue,
        maxValue,
        inclusive,
        isNullable,
        description
      )
    )
  }

  /** Check if one column is greater than a second column
    *
    * @param column1 name of first column that has greater value
    * @param column2 name of second column that has lesser value
    * @return
    */
  def isGreaterThan(column1: String, column2: String): RowLevelSchema = {

    val canEqual = false
    val isNullable: Boolean = false
    val description = "isGreaterThanAnotherColumn"
    RowLevelSchema(
      columnDefinitions :+ ColumnGreaterThanDefinition(
        column1,
        column2,
        canEqual,
        isNullable,
        description
      )
    )
  }

  /** Check if one column is greater than or equl to a second column
    *
    * @param column1 name of first column that has greater or equal value
    * @param column2 name of second column that has lesser or equal value
    * @return
    */
  def isGreaterThanOrEqualTo(
      column1: String,
      column2: String
  ): RowLevelSchema = {

    val canEqual = true
    val isNullable: Boolean = false
    val description = "isGreaterThanOrEqualToAnotherColumn"
    RowLevelSchema(
      columnDefinitions :+ ColumnGreaterThanDefinition(
        column1,
        column2,
        canEqual,
        isNullable,
        description
      )
    )
  }

  /** Check if one column is less than a second column
    *
    * @param column1 name of first column that has lesser value
    * @param column2 name of second column that has greater value
    * @return
    */
  def isLessThan(column1: String, column2: String): RowLevelSchema = {

    val canEqual = false
    val isNullable: Boolean = false
    val description = "isLessThanAnotherColumn"
    RowLevelSchema(
      columnDefinitions :+ ColumnLessThanDefinition(
        column1,
        column2,
        canEqual,
        isNullable,
        description
      )
    )
  }

  /** Check if one column is less than or equal to a second column
    *
    * @param column1 name of first column that has lesser or equal value
    * @param column2 name of second column that has greater or equal value
    * @return
    */
  def isLessThanOrEqualTo(column1: String, column2: String): RowLevelSchema = {

    val canEqual = true
    val isNullable: Boolean = false
    val description = "isGreaterThanOrEqualToAnotherColumn"
    RowLevelSchema(
      columnDefinitions :+ ColumnLessThanDefinition(
        column1,
        column2,
        canEqual,
        isNullable,
        description
      )
    )
  }
}

/** Result of enforcing a schema on textual data
  *
  * @param validRows  data frame holding the (casted) rows which conformed to the schema and checks
  * @param numValidRows number of rows which conformed to the schema
  * @param invalidRows data frame holding the rows which did not conform to the schema and checks
  * @param numInvalidRows number of rows which did not conform to the schema
  */
case class RowLevelValidationResult(
    validRows: DataFrame,
    numValidRows: Long,
    invalidRows: DataFrame,
    numInvalidRows: Long,
    stats: DataFrame
)

/** Enforce a schema on textual data */
object RowLevelSchemaValidator {

  private[this] val MATCHES_COLUMN = "failed_checks"
  private[this] val DATAFRAME = "dataframe"

  /** Enforces a schema and checks on textual data, filters out
    * non-conforming columns and casts the result to the requested types
    *
    * @param data a data frame holding the data to validate in string-typed columns
    * @param schema the schema and checks to enforce
    * @return results of schema and check enforcement
    */
  def validate(
      data: DataFrame,
      schema: RowLevelSchema,
      storageLevelForIntermediateResults: StorageLevel =
        StorageLevel.MEMORY_AND_DISK
  ): RowLevelValidationResult = {

    var nameList = Seq[String]()
    var dataWithMatches = data
    schema.columnDefinitions.foreach { definition =>
      val columnName = definition.name + definition.description
      nameList = nameList :+ columnName
      dataWithMatches =
        dataWithMatches.withColumn(columnName, toCNF(definition))
    }
    dataWithMatches =
      dataWithMatches.withColumn(MATCHES_COLUMN, lit(null: String))

    dataWithMatches.persist(storageLevelForIntermediateResults)
    dataWithMatches.createOrReplaceTempView(DATAFRAME)

    dataWithMatches = dataWithMatches.na.fill(false, nameList)
    val total_count_percent = 0.01 * dataWithMatches.count()
    var num_false = collection.mutable.Map[String, Double]()
    nameList.foreach { name =>
      num_false += (name -> dataWithMatches
        .filter(!dataWithMatches(name))
        .count() / (total_count_percent))
      dataWithMatches = dataWithMatches
        .withColumn(
          MATCHES_COLUMN,
          when(
            col(name) === false,
            concat_ws("|", lit(name), col(MATCHES_COLUMN))
          ).otherwise((col(MATCHES_COLUMN)))
        )
        .drop(col(name))
    }
    val invalidRows = dataWithMatches.na.drop(Seq(MATCHES_COLUMN))
    var validRows = dataWithMatches.filter(col(MATCHES_COLUMN).isNull)
    validRows = extractAndCastValidRows(validRows, schema)

    val numValidRows = validRows.count()
    val numInvalidRows = invalidRows.count()
    dataWithMatches.unpersist(false)

    num_false += ("Valid Rows %" -> numValidRows.toDouble / (total_count_percent))
    num_false += ("Invalid Rows %" -> numInvalidRows.toDouble / total_count_percent)

    val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    val stats = num_false.toSeq.toDF("Check_Name", "Percentage_Failed(%)")

    RowLevelValidationResult(
      validRows,
      numValidRows,
      invalidRows,
      numInvalidRows,
      stats
    )
  }

  private[this] def extractAndCastValidRows(
      dataWithMatches: DataFrame,
      schema: RowLevelSchema
  ): DataFrame = {

    val castExpressions = schema.columnDefinitions.map { colDef =>
      colDef.name -> colDef.castExpression()
    }.toMap

    val projection = dataWithMatches.schema
      .map { _.name }
      .filter { _ != MATCHES_COLUMN }
      .map { name => castExpressions.getOrElse(name, col(name)) }

    dataWithMatches.select(projection: _*)
    // .where(col(MATCHES_COLUMN))
  }

  private[this] def toCNF(columnDefinition: ColumnDefinition): Column = {
    var nextCnf = expr(true.toString)

    if (!columnDefinition.isNullable) {
      nextCnf = nextCnf.and(col(columnDefinition.name).isNotNull)
    }

    val colIsNull = col(columnDefinition.name).isNull

    columnDefinition match {

      case intDef: IntColumnDefinition =>
        val colAsInt = col(intDef.name).cast(IntegerType)

        /* null or successfully casted */
        nextCnf = nextCnf.and(colIsNull.or(colAsInt.isNotNull))

        intDef.minValue.foreach { value =>
          nextCnf = nextCnf.and(colIsNull.isNull.or(colAsInt.geq(value)))
        }

        intDef.maxValue.foreach { value =>
          nextCnf = nextCnf.and(colIsNull.or(colAsInt.leq(value)))
        }

      case decDef: DecimalColumnDefinition =>
        val decType =
          DataTypes.createDecimalType(decDef.precision, decDef.scale)
        nextCnf =
          nextCnf.and(colIsNull.or(col(decDef.name).cast(decType).isNotNull))

      case strDef: StringColumnDefinition =>
        strDef.minLength.foreach { value =>
          nextCnf =
            nextCnf.and(colIsNull.or(length(col(strDef.name)).geq(value)))
        }

        strDef.maxLength.foreach { value =>
          nextCnf =
            nextCnf.and(colIsNull.or(length(col(strDef.name)).leq(value)))
        }

        strDef.matches.foreach { regex =>
          nextCnf = nextCnf
            .and(
              colIsNull.or(
                regexp_extract(col(strDef.name), regex, 0).notEqual("")
              )
            )
        }

      case tsDef: TimestampColumnDefinition =>
        /* null or successfully casted */
        nextCnf = nextCnf.and(
          colIsNull.or(
            unix_timestamp(col(tsDef.name), tsDef.mask)
              .cast(TimestampType)
              .isNotNull
          )
        )

      case patternDef: PatternDefinition =>
        nextCnf = nextCnf
          .and(
            colIsNull.or(
              regexp_extract(col(patternDef.name), patternDef.pattern.regex, 0)
                .notEqual("")
            )
          )

      case nonNegDef: NonNegativeDefinition =>
        val colAsDec = col(nonNegDef.name).cast(IntegerType)
        nextCnf = nextCnf.and(colIsNull.or(colAsDec.geq(0.0)))

      case colGreaterDef: ColumnGreaterThanDefinition =>
        val colA = col(colGreaterDef.name)
        val colB = col(colGreaterDef.columnB)
        if (colGreaterDef.canEqual) {
          nextCnf = nextCnf.and(colIsNull.or(colA.geq(colB)))
        } else {
          nextCnf = nextCnf.and(colIsNull.or(colA.gt(colB)))
        }

      case greaterDef: GreaterThanDefinition =>
        val colAsInt = col(greaterDef.name).cast(IntegerType)

        if (greaterDef.canEqual) {
          nextCnf = nextCnf.and(colAsInt.geq(greaterDef.minValue))
        } else {
          nextCnf = nextCnf.and(colAsInt.gt(greaterDef.minValue))
        }

      case colLessDef: ColumnLessThanDefinition =>
        val colA = col(colLessDef.name)
        val colB = col(colLessDef.columnB)

        if (colLessDef.canEqual) {
          nextCnf = nextCnf.and(colIsNull.or(colA.leq(colB)))
        } else {
          nextCnf = nextCnf.and(colIsNull.or(colA.lt(colB)))
        }

      case lessDef: LessThanDefinition =>
        val colAsInt = col(lessDef.name).cast(IntegerType)

        if (lessDef.canEqual) {
          nextCnf = nextCnf.and(colAsInt.leq(lessDef.maxValue))
        } else {
          nextCnf = nextCnf.and(colAsInt.lt(lessDef.maxValue))
        }

      case rangeDef: RangeDefinition =>
        val colAsInt = col(rangeDef.name).cast(IntegerType)
        if (rangeDef.inclusive) {
          nextCnf = nextCnf.and(
            colIsNull.or(
              colAsInt
                .leq(rangeDef.maxValue)
                .and(colAsInt.geq(rangeDef.minValue))
            )
          )
        } else {
          nextCnf = nextCnf.and(
            colIsNull.or(
              colAsInt.lt(rangeDef.maxValue).and(colAsInt.gt(rangeDef.minValue))
            )
          )
        }

      case containDef: ContainedInDefinition =>
        val colname = col(containDef.name)
        var expression = s"(${colname} IS NULL)"
        for (value <- containDef.allowedValues) {
          expression = expression.concat(s" OR(${colname} IN ('${value}') )")
        }
    }
    nextCnf
  }
}
