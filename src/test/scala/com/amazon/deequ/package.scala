package com.amazon

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types.{ DataType => SparkDT }

package object deequ {
  def dataFrameWithColumn(
      name: String,
      columnType: SparkDT,
      sparkSession: SparkSession,
      values: Row*)
    : DataFrame = {

    import scala.collection.JavaConverters._
    val struct = StructType(StructField(name, columnType) :: Nil)
    sparkSession.createDataFrame(values.asJava, struct).toDF(name)
  }
}
