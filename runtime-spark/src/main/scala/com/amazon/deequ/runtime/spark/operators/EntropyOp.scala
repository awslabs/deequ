package com.amazon.deequ.runtime.spark.operators

import com.amazon.deequ.analyzers.ScanShareableFrequencyBasedAnalyzer
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, sum, udf}

/**
  * Entropy is a measure of the level of information contained in a message. Given the probability
  * distribution over values in a column, it describes how many bits are required to identify a
  * value.
  */
case class EntropyOp(column: String)
  extends ScanShareableFrequencyBasedOperator("Entropy", column :: Nil) {

  override def aggregationFunctions(numRows: Long): Seq[Column] = {
    val summands = udf { (count: Double) =>
      if (count == 0.0) {
        0.0
      } else {
        -(count / numRows) * math.log(count / numRows)
      }
    }

    sum(summands(col(Operators.COUNT_COL))) :: Nil
  }
}
