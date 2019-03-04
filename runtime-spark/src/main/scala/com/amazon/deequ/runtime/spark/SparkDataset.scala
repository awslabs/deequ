package com.amazon.deequ.runtime.spark

import com.amazon.deequ.runtime.Dataset
import org.apache.spark.sql.DataFrame

case class SparkDataset(df: DataFrame) extends Dataset
