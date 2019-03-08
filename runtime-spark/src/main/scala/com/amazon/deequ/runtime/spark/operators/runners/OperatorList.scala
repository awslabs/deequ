package com.amazon.deequ.runtime.spark.operators.runners

import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.runtime.spark.{StateLoader, StatePersister}
import com.amazon.deequ.runtime.spark.operators.Operator
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * Defines a set of analyzers to run on data.
  *
  * @param analyzers
  */
case class OperatorList(analyzers: Seq[Operator[_, Metric[_]]] = Seq.empty) {

  def addAnalyzer(analyzer: Operator[_, Metric[_]]): OperatorList = {
    OperatorList(analyzers :+ analyzer)
  }

  def addAnalyzers(otherAnalyzers: Seq[Operator[_, Metric[_]]]): OperatorList = {
    OperatorList(analyzers ++ otherAnalyzers)
  }

  /**
    * Compute the metrics from the analyzers configured in the analyis
    *
    * @param data data on which to operate
    * @param aggregateWith load existing states for the configured analyzers and aggregate them
    *                      (optional)
    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
    * @param storageLevelOfGroupedDataForMultiplePasses caching level for grouped data that must
    *                                                   be accessed multiple times (use
    *                                                   StorageLevel.NONE to completely disable
    *                                                   caching)
    * @return
    */
  @deprecated("Use the AnalysisRunner instead (the onData method there)", "24-09-2019")
  def run(
     data: DataFrame,
     aggregateWith: Option[StateLoader] = None,
     saveStatesWith: Option[StatePersister] = None,
     storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)
  : AnalyzerContext = {

    AnalysisRunner.doAnalysisRun(data, analyzers, aggregateWith = aggregateWith,
      saveStatesWith = saveStatesWith)
  }
}
