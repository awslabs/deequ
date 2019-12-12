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

import com.amazon.deequ.analyzers.{DataTypeInstances, KLLParameters}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.io.DfsUtils
import com.amazon.deequ.profiles.{ColumnProfile, ColumnProfilerRunner, ColumnProfiles}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.suggestions.rules._
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Rules {

  val DEFAULT: Seq[ConstraintRule[ColumnProfile]] =
    Seq(CompleteIfCompleteRule(), RetainCompletenessRule(), RetainTypeRule(),
      CategoricalRangeRule(), FractionalCategoricalRangeRule(),
      NonNegativeNumbersRule())
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
class ConstraintSuggestionRunner {

  def onData(data: DataFrame): ConstraintSuggestionRunBuilder = {
    new ConstraintSuggestionRunBuilder(data)
  }

  private[suggestions] def run(
      data: DataFrame,
      constraintRules: Seq[ConstraintRule[ColumnProfile]],
      restrictToColumns: Option[Seq[String]],
      lowCardinalityHistogramThreshold: Int,
      printStatusUpdates: Boolean,
      testsetWrapper: (Option[Double], Option[Long]),
      cacheInputs: Boolean,
      fileOutputOptions: ConstraintSuggestionFileOutputOptions,
      metricsRepositoryOptions: ConstraintSuggestionMetricsRepositoryOptions,
      kllWrapper: (Option[KLLParameters], Map[String, DataTypeInstances.Value]))
    : ConstraintSuggestionResult = {

    // get testset related data from wrapper
    val testsetRatio: Option[Double] = testsetWrapper._1
    val testsetSplitRandomSeed: Option[Long] = testsetWrapper._2

    val kllParameters: Option[KLLParameters] = kllWrapper._1
    val predefinedTypes: Map[String, DataTypeInstances.Value] = kllWrapper._2

    testsetRatio.foreach { testsetRatio =>
      require(testsetRatio > 0 && testsetRatio < 1.0, "Testset ratio must be in ]0, 1[")
    }

    val (trainingData, testData) = splitTrainTestSets(data, testsetRatio, testsetSplitRandomSeed)

    if (cacheInputs) {
      trainingData.cache()
      testData.foreach { _.cache() }
    }

    val (columnProfiles, constraintSuggestions) = ConstraintSuggestionRunner().profileAndSuggest(
      trainingData,
      constraintRules,
      restrictToColumns,
      lowCardinalityHistogramThreshold,
      printStatusUpdates,
      metricsRepositoryOptions,
      kllParameters,
      predefinedTypes
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
      .mapValues { groupedSuggestionsWithColumnNames =>
        groupedSuggestionsWithColumnNames.map { case (_, suggestion) => suggestion } }

    ConstraintSuggestionResult(columnProfiles.profiles, columnProfiles.numRecords,
      columnsWithSuggestions, verificationResult)
  }

  private[this] def splitTrainTestSets(
      data: DataFrame,
      testsetRatio: Option[Double],
      testsetSplitRandomSeed: Option[Long])
    : (DataFrame, Option[DataFrame]) = {

    if (testsetRatio.isDefined) {

      val trainsetRatio = 1.0 - testsetRatio.get
      val Array(trainSplit, testSplit) =
        if (testsetSplitRandomSeed.isDefined) {
          data.randomSplit(
            Array(trainsetRatio, testsetRatio.get),
            testsetSplitRandomSeed.get)
        } else {
          data.randomSplit(Array(trainsetRatio, testsetRatio.get))
        }
      (trainSplit, Some(testSplit))
    } else {
      (data, None)
    }
  }

  private[suggestions] def profileAndSuggest(
      trainingData: DataFrame,
      constraintRules: Seq[ConstraintRule[ColumnProfile]],
      restrictToColumns: Option[Seq[String]],
      lowCardinalityHistogramThreshold: Int,
      printStatusUpdates: Boolean,
      metricsRepositoryOptions: ConstraintSuggestionMetricsRepositoryOptions,
      kllParameters: Option[KLLParameters],
      predefinedTypes: Map[String, DataTypeInstances.Value])
    : (ColumnProfiles, Seq[ConstraintSuggestion]) = {

    var columnProfilerRunner = ColumnProfilerRunner()
      .onData(trainingData)
      .printStatusUpdates(printStatusUpdates)
      .withLowCardinalityHistogramThreshold(lowCardinalityHistogramThreshold)

    restrictToColumns.foreach { restrictToColumns =>
      columnProfilerRunner = columnProfilerRunner.restrictToColumns(restrictToColumns)
    }

    columnProfilerRunner = columnProfilerRunner.setKLLParameters(kllParameters)

    columnProfilerRunner =
      columnProfilerRunner.setPredefinedTypes(predefinedTypes)

    metricsRepositoryOptions.metricsRepository.foreach { metricsRepository =>
      var columnProfilerRunnerWithRepository = columnProfilerRunner.useRepository(metricsRepository)

      metricsRepositoryOptions.reuseExistingResultsKey.foreach { reuseExistingResultsKey =>
        columnProfilerRunnerWithRepository = columnProfilerRunnerWithRepository
          .reuseExistingResultsForKey(reuseExistingResultsKey,
            metricsRepositoryOptions.failIfResultsForReusingMissing)
      }

      metricsRepositoryOptions.saveOrAppendResultsKey.foreach { saveOrAppendResultsKey =>
        columnProfilerRunnerWithRepository = columnProfilerRunnerWithRepository
          .saveOrAppendResult(saveOrAppendResultsKey)
      }

      columnProfilerRunner = columnProfilerRunnerWithRepository
    }

    val profiles = columnProfilerRunner.run()

    val relevantColumns = getRelevantColumns(trainingData.schema, restrictToColumns)
    val suggestions = applyRules(constraintRules, profiles, relevantColumns)

    (profiles, suggestions)
  }

  private[this] def applyRules(
      constraintRules: Seq[ConstraintRule[ColumnProfile]],
      profiles: ColumnProfiles,
      columns: Seq[String])
    : Seq[ConstraintSuggestion] = {

    columns
      .flatMap { column =>

        val profile = profiles.profiles(column)

        constraintRules
          .filter { _.shouldBeApplied(profile, profiles.numRecords) }
          .map { _.candidate(profile, profiles.numRecords) }
      }
  }

  private[this] def getRelevantColumns(
      schema: StructType,
      restrictToColumns: Option[Seq[String]])
    : Seq[String] = {

    schema.fields
      .filter { field => restrictToColumns.isEmpty || restrictToColumns.get.contains(field.name) }
      .map { field => field.name }
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
      val constraints = constraintSuggestions.map { constraintSuggestion =>
        constraintSuggestion.constraint }
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

object ConstraintSuggestionRunner {

  def apply(): ConstraintSuggestionRunner = {
    new ConstraintSuggestionRunner()
  }
}
