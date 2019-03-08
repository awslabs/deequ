package com.amazon.deequ.runtime.spark.operators

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{Column, Row}
import Operators._
import Preconditions._

/** Completeness is the fraction of non-null values in a column of a DataFrame. */
case class CompletenessOp(column: String, where: Option[String] = None) extends
  StandardScanShareableOperator[NumMatchesAndCount]("Completeness", column) {

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatchesAndCount] = {

    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1))
    }
  }

  override def aggregationFunctions(): Seq[Column] = {

    val summation = sum(conditionalSelection(column, where).isNotNull.cast(IntegerType))

    summation :: conditionalCount(where) :: Nil
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: Nil
  }
}
