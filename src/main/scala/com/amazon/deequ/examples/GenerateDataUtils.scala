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

package com.amazon.deequ.examples

import scala.collection.mutable.ListBuffer
import scala.util.Random

case class DataProfile(size: Int, statusDist: Int, valuableDist: Int)

object GenerateDataUtils {

  val STATUSES = Array("IN_TRANSIT", "DELAYED", "UNKNOWN")
  val VALUABLES = Array("true", "false", null)
  val ran = new Random()

  var dataList = ListBuffer.empty[RawData]

  /**
   * Generates data according to DataProfile
   *  DataProfile.size: Total of rows
   *  DataProfile.statusDist: Minimum percent of rows with status equal to 'IN_TRANSIT'
   *  DataProfile.valuableDist: Minimum percent of rows with valuable equal to 'true'
   * @param dataProfile
   * @return
   */
  def generateData(dataProfile: DataProfile): Seq[RawData] = {
    if (dataProfile != null && dataProfile.size > 0) {
      for (i <- 1 to dataProfile.size)
        yield addData(
          RawData(
            "thing" + i,
            generateTotalNumber(50), // Generates values between 0(inclusive) & 50(exclusive)
            generateStatus(dataProfile), // Generates status according to DataProfile.statusDist
            generateValuable(dataProfile) // Generates valuable according to DataProfile.valuableDist
          )
        )
    }
    //showData(dataList)
    dataList.toSeq
  }

  // Generates totalNumber from zero inclusive to maxValue exclusive.
  def generateTotalNumber(maxValue: Int) : String = {
    ran.nextInt(maxValue).toFloat.toString
  }

  // Generates status according to statusDist, so that, statusDist will
  // determine the minimum percentage of rows with status in 'IN_TRANSIT'.
  def generateStatus(dataProfile: DataProfile): String = {
    val inTransitMax: Float = dataProfile.statusDist.toFloat / 100F
    val currentDataTot: Float = dataList.size.toFloat / dataProfile.size.toFloat
    if (currentDataTot <= inTransitMax) {
      STATUSES(0) // IN_TRANSIT
    } else {
      STATUSES(ran.nextInt(STATUSES.length)) // IN_TRANSIT, DELAYED or UNKNOWN
    }
  }

  // Generates the status according to valuableDist, so that, valuableDist
  // will determine the minimum percentage of rows with valuable in 'true'.
  def generateValuable(dataProfile: DataProfile): String = {
    val valuableMax: Float = dataProfile.valuableDist.toFloat / 100F
    val currentDataTot: Float = dataList.size.toFloat / dataProfile.size.toFloat
    if (currentDataTot <= valuableMax) {
      VALUABLES(0) // true
    } else {
      VALUABLES(ran.nextInt(VALUABLES.length)) // true, false or null
    }
  }

  def addData(rawData: RawData): Unit =
    dataList += rawData

  def showData(dataList: ListBuffer[RawData]): Unit = {
    dataList.foreach(rawData => {
      println(rawData)
    })
  }

}

