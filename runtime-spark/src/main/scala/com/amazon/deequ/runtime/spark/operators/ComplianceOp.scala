package com.amazon.deequ.runtime.spark.operators

import org.apache.spark.sql.{Column, Row}
import Operators._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  * Compliance is a measure of the fraction of rows that complies with the given column constraint.
  * E.g if the constraint is "att1>3" and data frame has 5 rows with att1 column value greater than
  * 3 and 10 rows under 3; a DoubleMetric would be returned with 0.33 value
  *
  * @param instance         Unlike other column analyzers (e.g completeness) this analyzer can not
  *                         infer to the metric instance name from column name.
  *                         Also the constraint given here can be referring to multiple columns,
  *                         so metric instance name should be provided,
  *                         describing what the analysis being done for.
  * @param predicate SQL-predicate to apply per row
  * @param where Additional filter to apply before the analyzer is run.
  */
case class ComplianceOp(instance: String, predicate: String, where: Option[String] = None)
  extends StandardScanShareableOperator[NumMatchesAndCount]("Compliance", instance) {

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatchesAndCount] = {

    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1))
    }
  }

  override def aggregationFunctions(): Seq[Column] = {

    val summation = sum(conditionalSelection(expr(predicate), where).cast(IntegerType))

    summation :: conditionalCount(where) :: Nil
  }
}
