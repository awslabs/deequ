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

package com.amazon.deequ.suggestions

import java.io.BufferedWriter

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.io.DfsUtils
import org.apache.spark.sql.SparkSession

object EndToEndConstraintSuggestion {

  def run(
      session: SparkSession,
      pathToData: String,
      resultsFolder: String,
      testsetRatio: Double,
      cacheInputs: Boolean = false,
      overwriteResults: Boolean = false)
    : Unit = {

    require(testsetRatio > 0 && testsetRatio < 1.0, "Testset ratio must be in ]0, 1[")

    val data = session.read.parquet(pathToData)

    val trainsetRatio = 1.0 - testsetRatio

    val Array(trainingData, testData) = data.randomSplit(Array(trainsetRatio, testsetRatio))

    if (cacheInputs) {
      trainingData.cache()
      testData.cache()
    }

    val suggestedConstraints = ConstraintSuggestions.suggest(trainingData)

    val profilesOutput = s"$resultsFolder/profile.json"
    val constraintsOutput = s"$resultsFolder/suggested-constraints.txt"
    val evaluationsOutput = s"$resultsFolder/evaluation.txt"

    println(s"### WRITING COLUMN PROFILES TO $profilesOutput")
    DfsUtils.writeToTextFileOnDfs(session, profilesOutput,
      overwrite = overwriteResults) { writer =>
        writer.append(ColumnProfiles.toJson(suggestedConstraints.columnProfiles).toString)
        writer.newLine()
      }

    if (cacheInputs) {
      trainingData.unpersist()
    }

    println(s"### WRITING CONSTRAINTS TO $constraintsOutput")
    DfsUtils.writeToTextFileOnDfs(session, constraintsOutput,
      overwrite = overwriteResults) { writer =>

        suggestedConstraints.constraints.foreach { constraint =>
          writer.append(constraint.toString())
          writer.newLine()
        }

        writer.newLine()
      }

    println("### RUNNING EVALUATION")
    val generatedCheck = Check(CheckLevel.Warning, "generated constraints",
      suggestedConstraints.constraints)

    val verificationResult = VerificationSuite()
        .onData(testData)
        .addCheck(generatedCheck)
        .run()

    println(s"### WRITING EVALUATION RESULTS TO $evaluationsOutput")
    DfsUtils.writeToTextFileOnDfs(session, evaluationsOutput,
      overwrite = overwriteResults) { writer =>
        writeEvaluationsResults(suggestedConstraints, verificationResult, writer)
      }
  }

  private[this] def writeEvaluationsResults(
      suggested: SuggestedConstraints,
      result: VerificationResult,
      writer: BufferedWriter)
    : Unit = {

    val results = result.checkResults
      .map { case (_, checkResult) => checkResult }
      .head
      .constraintResults

    writer.append("--- Suggested constraints ---")
    writer.newLine()
    writer.newLine()

    suggested.constraints.foreach { constraint =>
      writer.append(constraint.toString())
      writer.newLine()
    }

    writer.newLine()
    writer.append("--- Constraints which held on testset --------------------------")
    writer.newLine()
    writer.newLine()

    results
      .filter { _.status == ConstraintStatus.Success }
      .foreach { result =>
        writer.append(s"${result.constraint}")
        writer.newLine()
      }

    writer.newLine()
    writer.append("--- Constraints which failed on testset --------------------------")
    writer.newLine()
    writer.newLine()

    results
      .filter { _.status == ConstraintStatus.Failure }
      .foreach { result =>
        writer.append(s"${result.constraint}: ${result.message.get}")
        writer.newLine()
      }

    writer.newLine()
  }

}
