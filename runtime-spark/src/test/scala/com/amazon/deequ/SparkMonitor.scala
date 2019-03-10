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

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted, StageInfo}

/**
  * A class representing a statistics about a sparkSession.
  * Currently, only number of spark jobs submitted and its stages are being tracked.
  */
class SparkSessionStats {
  private var numberOfJobsSubmitted = 0
  private var stageInfos = Seq[StageInfo]()

  def jobCount: Int = {
    numberOfJobsSubmitted
  }

  def allExecutedStages: Seq[StageInfo] = {
    stageInfos
  }

  def recordJobStart(jobStart: SparkListenerJobStart): Unit = {
    numberOfJobsSubmitted += 1
  }

  def recordStageInfos(stageInfo: StageInfo): Unit = {
    stageInfos = stageInfos :+ stageInfo
  }

  def reset(): Unit = {
    numberOfJobsSubmitted = 0
    stageInfos = Seq[StageInfo]()
  }

}

/**
  * A SparkListener implementation to monitor spark jobs submitted
  */
class SparkMonitor extends SparkListener {
  val stat = new SparkSessionStats

  override def onJobStart(jobStart: SparkListenerJobStart) {
    stat.recordJobStart(jobStart)
    println(s"Job started with ${jobStart.stageInfos.size} stages: $jobStart " +
      s"details : ${jobStart.stageInfos.map(_.name)}")

  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stat.recordStageInfos(stageCompleted.stageInfo)
    println(s"Stage ${stageCompleted.stageInfo.stageId} completed with " +
      s"${stageCompleted.stageInfo.numTasks} tasks.")
  }

  /**
    * @param testFun thunk to run with SparkSessionStats as an argument.
    *                Provides a monitoring session where the stats are being reset at the beginning
    *
    */
  def withMonitoringSession(testFun: (SparkSessionStats) => Any): Any = {
    stat.reset
    testFun(stat)
  }

}
