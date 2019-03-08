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

package com.amazon.deequ

import com.amazon.deequ.checks.{Check, CheckStatus}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.runtime.{Dataset, Engine, EngineRepositoryOptions}
import com.amazon.deequ.statistics.Statistic

private[deequ] case class VerificationMetricsRepositoryOptions(
    metricsRepository: Option[MetricsRepository] = None,
    reuseExistingResultsForKey: Option[ResultKey] = None,
    failIfResultsForReusingMissing: Boolean = false,
    saveOrAppendResultsWithKey: Option[ResultKey] = None
)

//FIXLATER
//private[deequ] case class VerificationFileOutputOptions(
//                                                         sparkSession: Option[SparkSession] = None,
//                                                         saveCheckResultsJsonToPath: Option[String] = None,
//                                                         saveSuccessMetricsJsonToPath: Option[String] = None,
//                                                         overwriteOutputFiles: Boolean = false)

/** Responsible for running checks and required analysis and return the results */
class VerificationSuite {

  /**
    * Starting point to construct a VerificationRun.
    *
    * @param data tabular data on which the checks should be verified
    */
  def onData(data: Dataset, engine: Engine): VerificationRunBuilder = {
    new VerificationRunBuilder(data, engine)
  }



  /**
    * Runs all check groups and returns the verification result.
    * Verification result includes all the metrics computed during the run.
    *
    * @param data             tabular data on which the checks should be verified
    * @param checks           A sequence of check objects to be executed
    * @param requiredAnalyzers can be used to enforce the calculation of some some metrics
    *                          regardless of if there are constraints on them (optional)
    * @param aggregateWith    loader from which we retrieve initial states to aggregate (optional)
    * @param saveStatesWith   persist resulting states for the configured analyzers (optional)
    * @param metricsRepositoryOptions Options related to the MetricsRepository
    * //@param fileOutputOptions Options related to FileOuput using a SparkSession
    * @return Result for every check including the overall status, detailed status for each
    *         constraints and all metrics produced
    */
  private[deequ] def doVerificationRun(
      data: Dataset,
      engine: Engine,
      checks: Seq[Check],
      requiredAnalyzers: Seq[Statistic],
      //aggregateWith: Option[StateLoader] = None,
      //saveStatesWith: Option[StatePersister] = None,
      metricsRepositoryOptions: VerificationMetricsRepositoryOptions =
      VerificationMetricsRepositoryOptions())
//                                        fileOutputOptions: VerificationFileOutputOptions =
//                                        VerificationFileOutputOptions())
  : VerificationResult = {

    val analyzers = requiredAnalyzers ++ checks.flatMap { _.requiredAnalyzers() }

    val analysisResults = engine.compute(
      data,
      analyzers,
      //aggregateWith,
      //saveStatesWith,
      engineRepositoryOptions = EngineRepositoryOptions(
        metricsRepositoryOptions.metricsRepository,
        metricsRepositoryOptions.reuseExistingResultsForKey,
        metricsRepositoryOptions.failIfResultsForReusingMissing,
        saveOrAppendResultsWithKey = None))

    val verificationResult = evaluate(checks, analysisResults)

    val analyzerContext = ComputedStatistics(verificationResult.metrics)

    saveOrAppendResultsIfNecessary(
      analyzerContext,
      metricsRepositoryOptions.metricsRepository,
      metricsRepositoryOptions.saveOrAppendResultsWithKey)

    //FIXLATER
    //saveJsonOutputsToFilesystemIfNecessary(fileOutputOptions, verificationResult)

    verificationResult
  }

//FIXLATER
//  private[this] def saveJsonOutputsToFilesystemIfNecessary(
//                                                            fileOutputOptions: VerificationFileOutputOptions,
//                                                            verificationResult: VerificationResult)
//  : Unit = {
//
//    fileOutputOptions.sparkSession.foreach { session =>
//      fileOutputOptions.saveCheckResultsJsonToPath.foreach { profilesOutput =>
//
//        DfsUtils.writeToTextFileOnDfs(session, profilesOutput,
//          overwrite = fileOutputOptions.overwriteOutputFiles) { writer =>
//          writer.append(VerificationResult.checkResultsAsJson(verificationResult))
//          writer.newLine()
//        }
//      }
//    }
//
//    fileOutputOptions.sparkSession.foreach { session =>
//      fileOutputOptions.saveSuccessMetricsJsonToPath.foreach { profilesOutput =>
//
//        DfsUtils.writeToTextFileOnDfs(session, profilesOutput,
//          overwrite = fileOutputOptions.overwriteOutputFiles) { writer =>
//          writer.append(VerificationResult.successMetricsAsJson(verificationResult))
//          writer.newLine()
//        }
//      }
//    }
//  }

  private[this] def saveOrAppendResultsIfNecessary(
    resultingAnalyzerContext: ComputedStatistics,
    metricsRepository: Option[MetricsRepository],
    saveOrAppendResultsWithKey: Option[ResultKey])
  : Unit = {

    metricsRepository.foreach{repository =>
      saveOrAppendResultsWithKey.foreach { key =>

        val currentValueForKey = repository.loadByKey(key)

        // AnalyzerContext entries on the right side of ++ will overwrite the ones on the left
        // if there are two different metric results for the same analyzer
        val valueToSave = currentValueForKey.getOrElse(ComputedStatistics.empty) ++
          resultingAnalyzerContext

        repository.save(saveOrAppendResultsWithKey.get, valueToSave)
      }
    }
  }

//FIXLATER
//  /**
//    * Runs all check groups and returns the verification result. Metrics are computed from
//    * aggregated states. Verification result includes all the metrics generated during the run.
//    *
//    * @param schema schema of the tabular data on which the checks should be verified
//    * @param checks           A sequence of check objects to be executed
//    * @param stateLoaders loaders from which we retrieve the states to aggregate
//    * @param requiredAnalysis can be used to enforce the some metrics regardless of if
//    *                         there are constraints on them (optional)
//    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
//    * @return Result for every check including the overall status, detailed status for each
//    *         constraints and all metrics produced
//    */
//  def runOnAggregatedStates(
//                             schema: StructType,
//                             checks: Seq[Check],
//                             stateLoaders: Seq[StateLoader],
//                             requiredAnalysis: Analysis = Analysis(),
//                             saveStatesWith: Option[StatePersister] = None,
//                             metricsRepository: Option[MetricsRepository] = None,
//                             saveOrAppendResultsWithKey: Option[ResultKey] = None)
//  : VerificationResult = {
//
//    val analysis = requiredAnalysis.addAnalyzers(checks.flatMap { _.requiredAnalyzers() })
//
//    val analysisResults = AnalysisRunner.runOnAggregatedStates(
//      schema,
//      analysis,
//      stateLoaders,
//      saveStatesWith,
//      metricsRepository = metricsRepository,
//      saveOrAppendResultsWithKey = saveOrAppendResultsWithKey)
//
//    evaluate(checks, analysisResults)
//  }

//  /**
//    * Check whether a check is applicable to some data using the schema of the data.
//    *
//    * @param check A check that may be applicable to some data
//    * @param schema The schema of the data the checks are for
//    * @param sparkSession The spark session in order to be able to create fake data
//    */
//  def isCheckApplicableToData(
//                               check: Check,
//                               schema: StructType,
//                               sparkSession: SparkSession)
//  : CheckApplicability = {
//
//    new Applicability(sparkSession).isApplicable(check, schema)
//  }
//
//  /**
//    * Check whether analyzers are applicable to some data using the schema of the data.
//    *
//    * @param analyzers Analyzers that may be applicable to some data
//    * @param schema The schema of the data the analyzers are for
//    * @param sparkSession The spark session in order to be able to create fake data
//    */
//  def areAnalyzersApplicableToData(
//                                    analyzers: Seq[Analyzer[_ <: State[_], Metric[_]]],
//                                    schema: StructType,
//                                    sparkSession: SparkSession)
//  : AnalyzersApplicability = {
//
//    new Applicability(sparkSession).isApplicable(analyzers, schema)
//  }

  private[this] def evaluate(
    checks: Seq[Check],
    analysisContext: ComputedStatistics)
  : VerificationResult = {

    val checkResults = checks
      .map { check => check -> check.evaluate(analysisContext) }
      .toMap

    val verificationStatus = if (checkResults.isEmpty) {
      CheckStatus.Success
    } else {
      checkResults.values
        .map { _.status }
        .max
    }

    VerificationResult(verificationStatus, checkResults, analysisContext.metricMap)
  }
}

/** Convenience functions for using the VerificationSuite */
object VerificationSuite {

  def apply(): VerificationSuite = {
    new VerificationSuite()
  }

//FIXLATER
//  def runOnAggregatedStates(
//                             schema: StructType,
//                             checks: Seq[Check],
//                             stateLoaders: Seq[StateLoader],
//                             requiredAnalysis: Analysis = Analysis(),
//                             saveStatesWith: Option[StatePersister] = None,
//                             metricsRepository: Option[MetricsRepository] = None,
//                             saveOrAppendResultsWithKey: Option[ResultKey] = None                           )
//  : VerificationResult = {
//
//    VerificationSuite().runOnAggregatedStates(schema, checks, stateLoaders, requiredAnalysis,
//      saveStatesWith, metricsRepository, saveOrAppendResultsWithKey)
//  }
}