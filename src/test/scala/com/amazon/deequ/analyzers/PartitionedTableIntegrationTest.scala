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

package com.amazon.deequ.analyzers

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.utils.TempFileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.WordSpec
import TempFileUtils._
import com.amazon.deequ.{SparkContextSpec, VerificationResult, VerificationSuite}
import com.amazon.deequ.constraints.ConstraintStatus

class PartitionedTableIntegrationTest extends WordSpec with SparkContextSpec {

  private val SCHEMA = StructType(
    StructField("item", StringType, nullable = false) ::
    StructField("origin", StringType, nullable = true) ::
    StructField("sales", IntegerType, nullable = false) ::
    StructField("marketplace", StringType, nullable = false) :: Nil)

  "Constraint checks" should {
    "work correctly over data partitions" in withSparkSession { session =>

      // The check containing the constraints which we want to verify
      val check = Check(CheckLevel.Error, "table checks")
          .isComplete("item")
          .isComplete("origin")
          .isContainedIn("marketplace", Array("EU", "NA"))
          .isNonNegative("sales")

      val currentTime = System.currentTimeMillis()

      // We will store all computed metrics in this repository.
      // In a real implementation this would be backed by S3 or a database table
      val metricsRepository = FileSystemMetricsRepository(session, tempDir("metrics"))

      // The partitioned data on which we operate
      val partitionedData = Seq(
        "partitionA" -> loadPartitionA(session),
        "partitionB" -> loadPartitionB(session))

      // We run verification independently for all partitions and store metrics and states
      val partitionResultsAndStates = partitionedData
        .map { case (partitionName, data) =>

          // State store
          // If we want to persist the states, we could use an HdfsStateProvider
          val statesForPartition = InMemoryStateProvider()

          // Key under which the metrics of this partition will be stored
          val partitionMetricsKey = ResultKey(currentTime, Map("target" -> partitionName))

          val verificationResultsForPartition = VerificationSuite()
            .onData(data)
            .addCheck(check)
            .saveStatesWith(statesForPartition)
            .useRepository(metricsRepository)
            .saveOrAppendResult(partitionMetricsKey)
            .run()


          (verificationResultsForPartition, statesForPartition)
        }

      val partitionStates = partitionResultsAndStates
        .collect { case (_, states) => states }

      // Key under which the metrics of this partition will be stored
      val tableMetricsKey = ResultKey(currentTime, Map("target" -> "table"))

      // Run the verification for the whole table, re-using the states from the partitions
      val tableVerificationResult = VerificationSuite.runOnAggregatedStates(
        schema = SCHEMA,
        checks = Seq(check),
        stateLoaders = partitionStates,
        metricsRepository = Some(metricsRepository),
        saveOrAppendResultsWithKey = Some(tableMetricsKey)
      )


      // Investigate metrics and verification results for the table and the partitions
      printConstraintResults("table", tableVerificationResult)

      println("\n### METRICS FOR [table] ###")
      metricsRepository.loadByKey(tableMetricsKey)
        .get.metricMap
        .foreach { println }

      partitionedData.zip(partitionResultsAndStates)
        .foreach { case ((partitionName, _), (result, _)) =>

          printConstraintResults(partitionName, result)

          val partitionMetricsKey = ResultKey(currentTime, Map("target" -> partitionName))

          println(s"\n### METRICS FOR [$partitionName] ###")
          metricsRepository.loadByKey(partitionMetricsKey)
            .get.metricMap
            .foreach { println }
        }

    }
  }

  private[this] def printConstraintResults(entity: String, result: VerificationResult): Unit = {

    println(s"\n\n### CONSTRAINT RESULTS FOR [$entity] ###")
    println("\n\t--- Successful constraints ---")
    result.checkResults.foreach { case (_, checkResult) =>

      checkResult.constraintResults
        .filter { _.status == ConstraintStatus.Success }
        .foreach { constraintResult =>
          println(s"\t${constraintResult.constraint}")
        }
    }

    println("\n\t--- Failed constraints ---")
    result.checkResults.foreach { case (_, checkResult) =>

      checkResult.constraintResults
        .filter { _.status != ConstraintStatus.Success }
        .foreach { constraintResult =>
          println(s"\t${constraintResult.constraint}: ${constraintResult.message.get}")
        }
    }
  }

  private[this] def loadPartitionA(session: SparkSession): DataFrame = {

    val rowsInPartitionA = Seq(
      Row("item1", "US", 100, "EU"),
      Row("item2", "DE", 20, "EU"))

    session.createDataFrame(session.sparkContext.parallelize(rowsInPartitionA, 2), SCHEMA)
  }


  private[this] def loadPartitionB(session: SparkSession): DataFrame = {

    val rowsInPartitionB = Seq(
      Row("item1", "US", 1000, "NA"),
      Row("item2", "DE", 333, "typo"),
      Row("item3", null, 12, "NA"),
      Row("item4", null, 45, "NA"),
      Row("item5", null, 123, "NA"))

    session.createDataFrame(session.sparkContext.parallelize(rowsInPartitionB, 2), SCHEMA)
  }


}
