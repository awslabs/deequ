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

package com.amazon.deequ.profiles


import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.runtime.{Dataset, Engine}

private[profiles] case class ColumnProfilerRunBuilderMetricsRepositoryOptions(
    metricsRepository: Option[MetricsRepository],
    reuseExistingResultsKey: Option[ResultKey],
    failIfResultsForReusingMissing: Boolean,
    saveOrAppendResultsKey: Option[ResultKey]
)

private[profiles] case class ColumnProfilerRunBuilderFileOutputOptions(
    saveColumnProfilesJsonToPath: Option[String],
    overwriteResults: Boolean
)


class ColumnProfilerRunner {

  def onData(data: Dataset, engine: Engine): ColumnProfilerRunBuilder = {
    new ColumnProfilerRunBuilder(data, engine)
  }

  private[profiles] def run(
      data: Dataset,
      engine: Engine,
      restrictToColumns: Option[Seq[String]],
      lowCardinalityHistogramThreshold: Int,
      printStatusUpdates: Boolean,
//      cacheInputs: Boolean,
//      fileOutputOptions: ColumnProfilerRunBuilderFileOutputOptions,
      metricsRepositoryOptions: ColumnProfilerRunBuilderMetricsRepositoryOptions)
    : ColumnProfiles = {

    val columnProfiles = engine.profile(
      data,
      restrictToColumns,
      lowCardinalityHistogramThreshold,
      printStatusUpdates,
      metricsRepositoryOptions.metricsRepository,
      metricsRepositoryOptions.reuseExistingResultsKey,
      metricsRepositoryOptions.failIfResultsForReusingMissing,
      metricsRepositoryOptions.saveOrAppendResultsKey
    )

//    saveColumnProfilesJsonToFileSystemIfNecessary(
//      fileOutputOptions,
//      printStatusUpdates,
//      columnProfiles
//    )


    columnProfiles
  }

//  private[this] def saveColumnProfilesJsonToFileSystemIfNecessary(
//      fileOutputOptions: ColumnProfilerRunBuilderFileOutputOptions,
//      printStatusUpdates: Boolean,
//      columnProfiles: ColumnProfiles)
//    : Unit = {
//
//    fileOutputOptions.session.foreach { session =>
//      fileOutputOptions.saveColumnProfilesJsonToPath.foreach { profilesOutput =>
//        if (printStatusUpdates) {
//          println(s"### WRITING COLUMN PROFILES TO $profilesOutput")
//        }
//
//        DfsUtils.writeToTextFileOnDfs(session, profilesOutput,
//          overwrite = fileOutputOptions.overwriteResults) { writer =>
//            writer.append(ColumnProfiles.toJson(columnProfiles.profiles.values.toSeq).toString)
//            writer.newLine()
//          }
//        }
//    }
//  }
}

object ColumnProfilerRunner {
  def apply(): ColumnProfilerRunner = {
    new ColumnProfilerRunner()
  }
}
