package com.amazon.deequ.profiles

import com.amazon.deequ.io.DfsUtils
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, SparkSession}

private[profiles] case class ColumnProfilerRunBuilderStandardOptions(
      onlyConsiderColumnSubset: Option[Seq[String]],
      lowCardinalityHistogramThreshold: Int,
      printStatusUpdates: Boolean,
      cacheInputs: Boolean)

private[profiles] case class ColumnProfilerRunBuilderMetricsRepositoryOptions(
      metricsRepository: Option[MetricsRepository],
      reuseExistingResultsKey: Option[ResultKey],
      failIfResultsForReusingMissing: Boolean,
      saveOrAppendResultsKey: Option[ResultKey])

private[profiles] case class ColumnProfilerRunBuilderFileOutputOptions(
      session: Option[SparkSession],
      saveColumnProfilesJsonToPath: Option[String],
      saveConstraintSuggestionsJsonToPath: Option[String],
      saveEvaluationResultsJsonToPath: Option[String],
      overwriteResults: Boolean)

@Experimental
class ColumnProfilerRunner {

  def onData(data: DataFrame): ColumnProfilerRunBuilder = {
    new ColumnProfilerRunBuilder(data)
  }

  private[profiles] def run(
      data: DataFrame,
      standardOptions: ColumnProfilerRunBuilderStandardOptions,
      fileOutputOptions: ColumnProfilerRunBuilderFileOutputOptions,
      metricsRepositoryOptions: ColumnProfilerRunBuilderMetricsRepositoryOptions)
    : ColumnProfiles = {

    if (standardOptions.cacheInputs) {
      data.cache()
    }

    val columnProfiles = ColumnProfiler
      .profile(
        data,
        standardOptions.onlyConsiderColumnSubset,
        standardOptions.printStatusUpdates,
        standardOptions.lowCardinalityHistogramThreshold,
        metricsRepositoryOptions.metricsRepository,
        metricsRepositoryOptions.reuseExistingResultsKey,
        metricsRepositoryOptions.failIfResultsForReusingMissing,
        metricsRepositoryOptions.saveOrAppendResultsKey
      )

    saveColumnProfilesJsonToFileSystemIfNecessary(
      fileOutputOptions,
      standardOptions.printStatusUpdates,
      columnProfiles
    )

    if (standardOptions.cacheInputs) {
      data.unpersist()
    }

    columnProfiles
  }

  private[this] def saveColumnProfilesJsonToFileSystemIfNecessary(
      fileOutputOptions: ColumnProfilerRunBuilderFileOutputOptions,
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
}

object ColumnProfilerRunner {

  def apply(): ColumnProfilerRunner = {
    new ColumnProfilerRunner()
  }
}
