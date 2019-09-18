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

import org.apache.spark.sql.SparkSession

import scala.util.Random

case class ZipAndCity(zip: String, city: String, randomText: String)

object Play extends App {


  val records = generateData(10000)

  val session = SparkSession.builder()
    .master("local")
    .appName("test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", 2.toString)
    .getOrCreate()
  session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

  try {

    val df = session.createDataFrame(records)

    df.show()

    val predictabilityOfCity = Predictability.compute(df, Array("zip", "randomText"), "city")

    println(s"Predictability of city is: $predictabilityOfCity")


  } finally {
    session.stop()
    System.clearProperty("spark.driver.port")
  }



  def generateData(numRecords: Int): Seq[ZipAndCity] = {
    val citiesAndCodes: Seq[(String, Int, Int)] = Seq(
      ("Huntsville", 35801, 35816),
      ("Anchorage", 99501, 99524),
      ("Phoenix", 85001, 85055),
      ("Little Rock", 72201, 72217),
      ("Beverly Hills", 94203, 94209),
      ("Denver", 80201, 80239),
      ("Dover", 19901, 19905),
      ("Washington", 20001, 20020),
      ("Orlando", 32501, 32509),
      ("Atlanta", 30301, 30381),
      ("Honolulu", 96801, 96830),
      ("Wichita", 67201, 67221),
      ("Hazard", 41701, 41702),
      ("New Orleans", 70112, 70119),
      ("Baltimore", 21201, 21237))

    (0 until numRecords).map { _ =>
      val cityIndex = Random.nextInt(citiesAndCodes.length)
      val (city, lowZip, highZip) = citiesAndCodes(cityIndex)

      val range = highZip - lowZip
      val zip = lowZip + Random.nextInt(range)

      val randomTextLength = Random.nextInt(30)
      val randomText = (0 until randomTextLength).map { _ => Random.nextPrintableChar() }.mkString

      ZipAndCity(zip.toString, city, randomText)
    }

  }
}
