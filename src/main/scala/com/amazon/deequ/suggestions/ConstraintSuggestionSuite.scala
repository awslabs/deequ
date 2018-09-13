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

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.io.DfsUtils
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.suggestions.rules._
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, SparkSession}

object Rules {

  val ALL: Seq[ConstraintRule[ColumnProfile]] =
    Seq(CompleteIfCompleteRule, RetainCompletenessRule, UniqueIfApproximatelyUniqueRule,
      RetainTypeRule, CategoricalRangeRule, FractionalCategoricalRangeRule, NonNegativeNumbersRule,
      PositiveNumbersRule)
}

private[suggestions] case class ConstraintSuggestionMetricsRepositoryOptions(
      metricsRepository: Option[MetricsRepository],
      reuseExistingResultsKey: Option[ResultKey],
      failIfResultsForReusingMissing: Boolean,
      saveOrAppendResultsKey: Option[ResultKey])

private[suggestions] case class ConstraintSuggestionFileOutputOptions(
      session: Option[SparkSession],
      saveColumnProfilesJsonToPath: Option[String],
      saveConstraintSuggestionsJsonToPath: Option[String],
      saveEvaluationResultsJsonToPath: Option[String],
      overwriteResults: Boolean)

/**
  * Generate suggestions for constraints by applying the rules on the column profiles computed from
  * the data at hand.
  *
  */
@Experimental
class ConstraintSuggestionSuite {

  def onData(data: DataFrame): ConstraintSuggestionRunBuilder = {
    new ConstraintSuggestionRunBuilder(data)
  }

  private[suggestions] def run(
      data: DataFrame,
      constraintRules: Seq[ConstraintRule[ColumnProfile]],
      fromColumns: Option[Array[String]],
      lowCardinalityHistogramThreshold: Int,
      printStatusUpdates: Boolean,
      testsetRatio: Option[Double],
      testsetSplitRandomSeed: Option[Long],
      cacheInputs: Boolean,
      fileOutputOptions: ConstraintSuggestionFileOutputOptions,
      metricsRepositoryOptions: ConstraintSuggestionMetricsRepositoryOptions)
    : ConstraintSuggestionResult = {

    testsetRatio.foreach { testsetRatio =>
      require(testsetRatio > 0 && testsetRatio < 1.0, "Testset ratio must be in ]0, 1[")
    }

    var testData: Option[DataFrame] = None

    val trainingData = if (testsetRatio.isDefined) {

      val trainsetRatio = 1.0 - testsetRatio.get
      val Array(trainingData, testSplit) = if (testsetSplitRandomSeed.isDefined) {
        data.randomSplit(Array(trainsetRatio, testsetRatio.get), testsetSplitRandomSeed.get)
      } else {
         data.randomSplit(Array(trainsetRatio, testsetRatio.get))
      }
      testData = Some(testSplit)
      trainingData
    } else {
      data
    }

    if (cacheInputs) {
      trainingData.cache()
      testData.foreach { _.cache() }
    }

    val (columnProfiles, constraintSuggestions) = ConstraintSuggestionSuite()
      .profileAndSuggest(
        trainingData,
        constraintRules,
        fromColumns,
        lowCardinalityHistogramThreshold,
        printStatusUpdates,
        metricsRepositoryOptions
      )

    saveColumnProfilesJsonToFileSystemIfNecessary(
      fileOutputOptions,
      printStatusUpdates,
      columnProfiles
    )

    if (cacheInputs) {
      trainingData.unpersist()
    }

    saveConstraintSuggestionJsonToFileSystemIfNecessary(
      fileOutputOptions,
      printStatusUpdates,
      constraintSuggestions
    )

    val verificationResult = evaluateConstraintsIfNecessary(
      testData,
      printStatusUpdates,
      constraintSuggestions,
      fileOutputOptions
    )

    val columnsWithSuggestions = constraintSuggestions
      .map(suggestion => suggestion.columnName -> suggestion)
      .groupBy { case (columnName, _) => columnName }
      .mapValues { case entry => entry.map(x => x._2) }

    ConstraintSuggestionResult(columnProfiles.profiles, columnsWithSuggestions, verificationResult)
  }

  private[suggestions] def profileAndSuggest(
      trainingData: DataFrame,
      constraintRules: Seq[ConstraintRule[ColumnProfile]],
      fromColumns: Option[Array[String]],
      lowCardinalityHistogramThreshold: Int,
      printStatusUpdates: Boolean,
      metricsRepositoryOptions: ConstraintSuggestionMetricsRepositoryOptions)
    : (ColumnProfiles, Seq[ConstraintSuggestion]) = {

    val interestingColumns = fromColumns.getOrElse(trainingData.schema.fieldNames)

    val profiles = ColumnProfiler.profile(
      trainingData,
      interestingColumns,
      printStatusUpdates,
      lowCardinalityHistogramThreshold,
      metricsRepository = metricsRepositoryOptions.metricsRepository,
      failIfResultsForReusingMissing = metricsRepositoryOptions.failIfResultsForReusingMissing,
      reuseExistingResultsUsingKey = metricsRepositoryOptions
        .reuseExistingResultsKey,
      saveInMetricsRepositoryUsingKey = metricsRepositoryOptions.saveOrAppendResultsKey
    )

    val suggestions = applyRules(constraintRules, profiles, interestingColumns)

    (profiles, suggestions)
  }

  private[this] def applyRules(
      constraintRules: Seq[ConstraintRule[ColumnProfile]],
      profiles: ColumnProfiles,
      columns: Array[String])
    : Seq[ConstraintSuggestion] = {

    columns
      .filter { name => profiles.profiles.contains(name) }
      .flatMap { column =>

        val profile = profiles.profiles(column)

        constraintRules
          .filter { _.shouldBeApplied(profile, profiles.numRecords) }
          .map { _.candidate(profile, profiles.numRecords) }
      }
  }

  private[this] def saveColumnProfilesJsonToFileSystemIfNecessary(
      fileOutputOptions: ConstraintSuggestionFileOutputOptions,
      printStatusUpdates: Boolean,
      columnProfiles: ColumnProfiles)
    : Unit = {

    fileOutputOptions.session.foreach { session =>
      fileOutputOptions.saveColumnProfilesJsonToPath.foreach { profilesOutput =>
        if (printStatusUpdates) {
          println(s"### WRITING COLUMN PROFILES TO $profilesOutput")
        }

        DfsUtils.writeToTextFileOnDfs(session, profilesOutput,
          overwrite = fileOutputOptions.overwriteResults) { writer =>
            writer.append(ColumnProfiles.toJson(columnProfiles.profiles.values.toSeq).toString)
            writer.newLine()
          }
        }
    }
  }

  private[this] def saveConstraintSuggestionJsonToFileSystemIfNecessary(
      fileOutputOptions: ConstraintSuggestionFileOutputOptions,
      printStatusUpdates: Boolean,
      constraintSuggestions: Seq[ConstraintSuggestion])
    : Unit = {

    fileOutputOptions.session.foreach { session =>
      fileOutputOptions.saveConstraintSuggestionsJsonToPath.foreach { constraintsOutput =>
        if (printStatusUpdates) {
          println(s"### WRITING CONSTRAINTS TO $constraintsOutput")
        }
        DfsUtils.writeToTextFileOnDfs(session, constraintsOutput,
          overwrite = fileOutputOptions.overwriteResults) { writer =>
            writer.append(ConstraintSuggestions.toJson(constraintSuggestions).toString)
            writer.newLine()
          }
      }
    }
  }

  private[this] def saveEvaluationResultJsonToFileSystemIfNecessary(
      fileOutputOptions: ConstraintSuggestionFileOutputOptions,
      printStatusUpdates: Boolean,
      constraintSuggestions: Seq[ConstraintSuggestion],
      verificationResult: VerificationResult)
    : Unit = {

    fileOutputOptions.session.foreach { session =>
        fileOutputOptions.saveEvaluationResultsJsonToPath.foreach { evaluationsOutput =>
          if (printStatusUpdates) {
            println(s"### WRITING EVALUATION RESULTS TO $evaluationsOutput")
          }
          DfsUtils.writeToTextFileOnDfs(session, evaluationsOutput,
            overwrite = fileOutputOptions.overwriteResults) { writer =>
            writer.append(ConstraintSuggestions
              .evaluationResultsToJson(constraintSuggestions, verificationResult))
            writer.newLine()
          }
        }
      }
  }

  private[this] def evaluateConstraintsIfNecessary(
     testData: Option[DataFrame],
     printStatusUpdates: Boolean,
     constraintSuggestions: Seq[ConstraintSuggestion],
     fileOutputOptions: ConstraintSuggestionFileOutputOptions)
    : Option[VerificationResult] = {

    if (testData.isDefined) {
      if (printStatusUpdates) {
        println("### RUNNING EVALUATION")
      }
      val constraints = constraintSuggestions.map(constraintSuggestion =>
        constraintSuggestion.constraint)
      val generatedCheck = Check(CheckLevel.Warning, "generated constraints", constraints)

      val verificationResult = VerificationSuite()
        .onData(testData.get)
        .addCheck(generatedCheck)
        .run()

      saveEvaluationResultJsonToFileSystemIfNecessary(
        fileOutputOptions,
        printStatusUpdates,
        constraintSuggestions,
        verificationResult)

      Option(verificationResult)
    } else {
      None
    }
  }

}

object ConstraintSuggestionSuite {

  def apply(): ConstraintSuggestionSuite = {
    new ConstraintSuggestionSuite()
  }
}
