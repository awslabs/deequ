package com.amazon.deequ.schema

import org.apache.spark.sql.functions.{col, lit, expr, length, not, unix_timestamp, regexp_extract, when, concat, concat_ws}
import org.apache.spark.sql.types.{DataTypes, DecimalType, IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import com.amazon.deequ.analyzers.{Patterns}
import scala.util.matching.Regex

sealed trait CheckDefinition {
    def name: String
    def isNullable: Boolean
    def description: String
}

case class PatternDefinition(
    name: String,
    isNullable: Boolean,
    pattern: Regex,
    description: String) extends CheckDefinition

case class NonNegativeDefinition(
    name: String,
    isNullable: Boolean,
    description: String) extends CheckDefinition {
    }

case class ColumnGreaterThanDefinition(
    name: String,
    columnB: String,
    canEqual: Boolean,
    isNullable: Boolean,
    description: String) extends CheckDefinition {
    }

case class GreaterThanDefinition(       
    name: String,
    minValue: Int,
    canEqual: Boolean,
    isNullable: Boolean,
    description: String) extends CheckDefinition {
    }

case class ColumnLessThanDefinition(
    name: String,
    columnB: String,
    canEqual: Boolean,
    isNullable: Boolean,
    description: String) extends CheckDefinition {
    }

case class LessThanDefinition(       
    name: String,
    maxValue: Int,
    canEqual: Boolean,
    isNullable: Boolean,
    description: String) extends CheckDefinition {
    }

case class RangeDefinition(
    name: String,
    minValue: Int,
    maxValue: Int,
    inclusive: Boolean,
    isNullable: Boolean,
    description: String) extends CheckDefinition {
    }

case class ContainedInDefinition(
    name: String,
    allowedValues: Array[String],
    isNullable: Boolean,
    description: String) extends CheckDefinition {
    }



case class RowLevelChecks(checkDefinitions: Seq[CheckDefinition] = Seq.empty) {

/**
    * Checks if String contains email pattern
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
    def containsEmail(
        name: String,
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "ContainsEmailCheck"
        RowLevelChecks(checkDefinitions :+ PatternDefinition(name, isNullable, Patterns.EMAIL, description))
        }

/**
    * Check if string contains social security number
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
    def containsSocialSecurityNumber(
        name: String,
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "ContainsSSNCheck"
        RowLevelChecks(checkDefinitions :+ PatternDefinition(name, isNullable, Patterns.SOCIAL_SECURITY_NUMBER_US, description))
        }

/**
    * Check if string contains URL pattern
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
    def containsURL(
        name: String,
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "ContainsURLCheck"
        RowLevelChecks(checkDefinitions :+ PatternDefinition(name, isNullable, Patterns.URL, description))
        }

/**
    * Check if string contains credit card number
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
    def containsCreditCardNumber(
        name: String,
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "ContainsCreditCardNumberCheck"
        RowLevelChecks(checkDefinitions :+ PatternDefinition(name, isNullable, Patterns.CREDITCARD, description))
        }

/**
    * Check if string is contained in a set array of string values
    *
    * @param name column name
    * @param allowedValues array of values to check if string contained in
    * @param isNullable are NULL values permitted?
    * @return
    */
    def isContainedIn(
        name: String,
        allowedValues: Array[String],
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "isContainedInCheck"
        RowLevelChecks(checkDefinitions :+ ContainedInDefinition(name, allowedValues, isNullable, description))
    }

/**
    * Check if int column is nonnegative
    *
    * @param name column name
    * @param isNullable are NULL values permitted?
    * @return
    */
    def isNonNegative(
        name: String,
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "isNonNegativeCheck"
        RowLevelChecks(checkDefinitions :+ NonNegativeDefinition(name, isNullable, description))
    }

/**
    * Check if int column is greater than a minimum value
    *
    * @param name column name
    * @param minValue minimum value integer must be larger than 
    * @param isNullable are NULL values permitted?
    * @return
    */
    def isGreaterThan(
        name: String,
        minValue: Int,
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "isGreaterThanCheck"
        val canEqual = false
        RowLevelChecks(checkDefinitions :+ GreaterThanDefinition(name, minValue, canEqual, isNullable, description))
    }

/**
    * Check if int column is greater than or equal to a minimum value
    *
    * @param name column name
    * @param minValue minimum value that row value must be larger than or equal to
    * @param isNullable are NULL values permitted?
    * @return
    */
    def isGreaterThanOrEqualTo(
        name: String,
        minValue: Int,
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "isGreaterThanorEqualToCheck"
        val canEqual = false
        RowLevelChecks(checkDefinitions :+ GreaterThanDefinition(name, minValue, canEqual, isNullable, description))
    }

/**
    * Check if int column is less than a maximum value
    *
    * @param name column name
    * @param maxValue maximum value that row value must be less than 
    * @param isNullable are NULL values permitted?
    * @return
    */
    def isLessThan(
        name: String,
        maxValue: Int,
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "isLessThanCheck"
        val canEqual = false
        RowLevelChecks(checkDefinitions :+ LessThanDefinition(name, maxValue, canEqual, isNullable, description))
    }

/**
    * Check if int column is less than or equal to a maximum value
    *
    * @param name column name
    * @param maxValue maximum value that row value must be less than or equal to
    * @param isNullable are NULL values permitted?
    * @return
    */
    def isLessThanOrEqualTo(
        name: String,
        maxValue: Int,
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "isLessThanOrEqualToCheck"
        val canEqual = false
        RowLevelChecks(checkDefinitions :+ LessThanDefinition(name, maxValue, canEqual, isNullable, description))
    }

/**
    * Check if int column is in a range of values 
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
        isNullable: Boolean = true)
        : RowLevelChecks = {
        val description = "isInRangeCheck"
        RowLevelChecks(checkDefinitions :+ RangeDefinition(name, minValue, maxValue, inclusive, isNullable, description))
    }

/**
    * Check if one column is greater than a second column
    *
    * @param column1 name of first column that has greater value
    * @param column2 name of second column that has lesser value
    * @return
    */
    def isGreaterThan(
        column1: String,
        column2: String)
        : RowLevelChecks = {
        
        val canEqual = false
        val isNullable: Boolean = false
        val description = "isGreaterThanAnotherColumn"
        RowLevelChecks(checkDefinitions :+ ColumnGreaterThanDefinition(column1, column2, canEqual, isNullable, description))
    }

/**
    * Check if one column is greater than or equl to a second column
    *
    * @param column1 name of first column that has greater or equal value
    * @param column2 name of second column that has lesser or equal value
    * @return
    */
    def isGreaterThanOrEqualTo(
        column1: String,
        column2: String)
        : RowLevelChecks = {

        val canEqual = true
        val isNullable: Boolean = false
        val description = "isGreaterThanOrEqualToAnotherColumn"
        RowLevelChecks(checkDefinitions :+ ColumnGreaterThanDefinition(column1, column2, canEqual, isNullable, description))
    }

/**
    * Check if one column is less than a second column
    *
    * @param column1 name of first column that has lesser value
    * @param column2 name of second column that has greater value
    * @return
    */
    def isLessThan(
        column1: String,
        column2: String)
        : RowLevelChecks = {

        val canEqual = false
        val isNullable: Boolean = false
        val description = "isLessThanAnotherColumn"
        RowLevelChecks(checkDefinitions :+ ColumnLessThanDefinition(column1, column2, canEqual, isNullable, description))
    }

/**
    * Check if one column is less than or equal to a second column
    *
    * @param column1 name of first column that has lesser or equal value
    * @param column2 name of second column that has greater or equal value
    * @return
    */
    def isLessThanOrEqualTo(
        column1: String,
        column2: String)
        : RowLevelChecks = {

        val canEqual = true
        val isNullable: Boolean = false
        val description = "isGreaterThanOrEqualToAnotherColumn"
        RowLevelChecks(checkDefinitions :+ ColumnLessThanDefinition(column1, column2, canEqual, isNullable, description))
    }
}

/**
  * Result of enforcing a checks on textual data
  *
  * @param validRows  data frame holding the rows which conformed to the checks
  * @param numValidRows number of rows which conformed to the checks
  * @param invalidRows data frame holding the rows which did not conform to the checks
  * @param numInvalidRows number of rows which did not conform to the checks
  */
case class RowValidationResult(
  validRows: DataFrame,
  numValidRows: Long,
  invalidRows: DataFrame,
  numInvalidRows: Long
)

/** Enforce a checks on textual data */
object RowValidator {

    /**
        * Enforces a checks on textual data, filters out non-conforming columns
        *
        * @param data a data frame holding the data to validate in string-typed columns
        * @param checks the checks to enforce
        * @param storageLevelForIntermediateResults the storage level for intermediate results
        *                                           (to control caching behavior)
        * @return results of checks enforcement
        */
    def validate(
        data: DataFrame,
        checks: RowLevelChecks,
        storageLevelForIntermediateResults: StorageLevel = StorageLevel.MEMORY_AND_DISK
        ): RowValidationResult = {

        var nameList = Seq[String]()
        var dataWithMatches = data
        checks.checkDefinitions.foreach{ definition =>
            val columnName = definition.name + definition.description
            nameList = nameList :+ columnName
            dataWithMatches = dataWithMatches.withColumn(columnName, toCNF(definition))
        }
        val failedColumn = "failed_checks"
        dataWithMatches = dataWithMatches.withColumn(failedColumn, lit(null: String))

        dataWithMatches.persist(storageLevelForIntermediateResults)
        val dfName = "dataframe"
        dataWithMatches.createOrReplaceTempView(dfName)
        var sqlExp = s"SELECT * FROM ${dfName}" 
        var count = 0
        nameList.foreach{ name =>
            if (count < 1){
                sqlExp = sqlExp + s" WHERE ${name}='true'"
            }else{
                sqlExp = sqlExp + s" AND ${name}='true'"
            }
            
            count = count + 1
        }
        
        var validRows = spark.sql(sqlExp)
        nameList.foreach{ name =>
            validRows = validRows.drop(name)
        }
        validRows = validRows.drop(failedColumn)
        val numValidRows = validRows.count()

        sqlExp = s"SELECT * FROM ${dfName}" 
        count = 0
        nameList.foreach{ name =>
            if (count < 1){
                sqlExp = sqlExp + s" WHERE ${name}='false'"
            }else{
                sqlExp = sqlExp + s" OR ${name}='false'"
            }
            
            count = count + 1
        }
            
        var invalidRows: DataFrame = spark.sql(sqlExp)
        var sqlExp2 = s"Select * FROM ${dfName}"
        nameList.foreach{ name =>

            invalidRows = invalidRows.withColumn(failedColumn, when(col(name) === false, concat_ws("|" , lit(name), col(failedColumn))).otherwise((col(failedColumn))))
                .drop(col(name))
        }

        val numInValidRows = invalidRows.count()
        dataWithMatches.unpersist(false)

        RowValidationResult(validRows, numValidRows, invalidRows, numInValidRows)
    }

    private[this] def toCNF(checkDefinition: CheckDefinition): Column = {

        var nextCnf = expr(true.toString)
        if (!checkDefinition.isNullable) {
            nextCnf = nextCnf.and(col(checkDefinition.name).isNotNull)
        } 

        val colIsNull = col(checkDefinition.name).isNull

        checkDefinition match {

            case patternDef: PatternDefinition =>
                nextCnf = nextCnf
                    .and(colIsNull.or(regexp_extract(col(patternDef.name), patternDef.pattern.regex, 0).notEqual("")))

            case nonNegDef: NonNegativeDefinition =>
                val colAsDec = col(nonNegDef.name).cast(IntegerType)
                nextCnf = nextCnf.and(colIsNull.or(colAsDec.geq(0.0)))

            case colGreaterDef : ColumnGreaterThanDefinition =>
                val colA = col(colGreaterDef.name)
                val colB = col(colGreaterDef.columnB)

                if (colGreaterDef.canEqual) {
                    nextCnf = nextCnf.and(colIsNull.or(colA.geq(colB)))
                }else{
                    nextCnf = nextCnf.and(colIsNull.or(colA.gt(colB)))
                }

            case greaterDef : GreaterThanDefinition =>
                val colAsInt = col(greaterDef.name).cast(IntegerType)

                if (greaterDef.canEqual) {
                    nextCnf = nextCnf.and(colAsInt.geq(greaterDef.minValue))
                }else{
                    nextCnf = nextCnf.and(colAsInt.gt(greaterDef.minValue)) 
                                }

            case colLessDef : ColumnLessThanDefinition =>
                val colA = col(colLessDef.name)
                val colB = col(colLessDef.columnB)

                if (colLessDef.canEqual) {
                    nextCnf = nextCnf.and(colIsNull.or(colA.leq(colB)))
                }else{
                    nextCnf = nextCnf.and(colIsNull.or(colA.lt(colB)))
                }
            
            case lessDef : LessThanDefinition =>
                val colAsInt = col(lessDef.name).cast(IntegerType)

                if (lessDef.canEqual) {
                    nextCnf = nextCnf.and(colAsInt.leq(lessDef.maxValue))
                }else{
                    nextCnf = nextCnf.and(colAsInt.lt(lessDef.maxValue)) 
                }

            case rangeDef : RangeDefinition =>
                val colAsInt = col(rangeDef.name).cast(IntegerType)
                if (rangeDef.inclusive) {
                    nextCnf = nextCnf.and(colIsNull.or(colAsInt.leq(rangeDef.maxValue).and(colAsInt.geq(rangeDef.minValue))))
                }else{
                    nextCnf = nextCnf.and(colIsNull.or(colAsInt.lt(rangeDef.maxValue).and(colAsInt.gt(rangeDef.minValue))))
                }

            case containDef : ContainedInDefinition =>
                val colname = col(containDef.name)
                var expression = s"(${colname} IS NULL)"
                for (value <- containDef.allowedValues) {
                        expression = expression.concat(s" OR(${colname} IN ('${value}') )")
                    }
                nextCnf = nextCnf.and(colIsNull.or(expr(expression)))

        }
        nextCnf
        
    }
}



