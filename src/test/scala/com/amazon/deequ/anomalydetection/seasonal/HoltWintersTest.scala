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

package com.amazon.deequ.anomalydetection.seasonal

import com.amazon.deequ.anomalydetection.Anomaly
import org.scalatest.{Matchers, WordSpec}

class HoltWintersTest extends WordSpec with Matchers {

  import HoltWintersTest._

  "Additive Holt-Winters" should {
    val rng = new util.Random(seed = 42L)
    val twoWeeksOfData = Vector.fill(2)(
      Vector[Double](1, 1, 1.2, 1.3, 1.5, 2.1, 1.9)
    ).flatten.map(_ + rng.nextGaussian())

    "fail if start after or equal to end" in {
      val caught = intercept[IllegalArgumentException](
        dailyMetricsWithWeeklySeasonalityAnomalies(twoWeeksOfData, 1 -> 1))

      caught.getMessage shouldBe "requirement failed: Start must be before end"
    }

    "fail if no at least two cycles are available" in {
      val fullInterval = 0 -> Int.MaxValue

      val caught = intercept[IllegalArgumentException](
        dailyMetricsWithWeeklySeasonalityAnomalies(Vector.empty, fullInterval))

      caught.getMessage shouldBe "requirement failed: Provided data series is empty"
    }

    "fail for negative search interval" in {
      val negativeInterval = -2 -> -1

      val caught = intercept[IllegalArgumentException](
        dailyMetricsWithWeeklySeasonalityAnomalies(twoWeeksOfData, negativeInterval))

      caught.getMessage shouldBe
        "requirement failed: The search interval needs to be strictly positive"
    }

    "fail for too few data" in {
      val fullInterval = 0 -> Int.MaxValue
      val shortSeries = Vector[Double](1, 2, 3)

      val caught = intercept[IllegalArgumentException](
        dailyMetricsWithWeeklySeasonalityAnomalies(shortSeries, fullInterval))

      caught.getMessage shouldBe
        "requirement failed: Need at least two full cycles of data to estimate model"
    }

    "run anomaly detection on the last data point if search interval beyond series size" in {
      val interval = 100 -> 110
      val anomalies = dailyMetricsWithWeeklySeasonalityAnomalies(twoWeeksOfData, interval)
      anomalies shouldBe empty
    }

    "predict no anomaly for normally distributed errors" in {
      val seriesWithOutlier = twoWeeksOfData ++ Vector(twoWeeksOfData.head)
      val anomalies = dailyMetricsWithWeeklySeasonalityAnomalies(seriesWithOutlier, 14 -> 15)
      anomalies shouldBe empty
    }

    "predict an anomaly" in {
      val seriesWithOutlier = twoWeeksOfData ++ Vector(0.0d)
      val anomalies = dailyMetricsWithWeeklySeasonalityAnomalies(
        seriesWithOutlier, 14 -> Int.MaxValue)

      anomalies should have size 1
      val (anomalyIndex, _) = anomalies.head
      anomalyIndex shouldBe 14
    }

    "predict no anomalies on longer series" in {
      val seriesWithOutlier = twoWeeksOfData ++ twoWeeksOfData
      val anomalies = dailyMetricsWithWeeklySeasonalityAnomalies(
        seriesWithOutlier, 26 -> Int.MaxValue)
      anomalies shouldBe empty
    }

    "detect no anomalies on constant series" in {
      val series = (0 until 21).map(_ => 1.0).toVector
      val anomalies = dailyMetricsWithWeeklySeasonalityAnomalies(series, 14 -> Int.MaxValue)
      anomalies shouldBe empty
    }

    "detect a single anomaly in constant series with a single error" in {
      val series = ((0 until 20).map(_ => 1.0) ++ Seq(0.0)).toVector
      val anomalies = dailyMetricsWithWeeklySeasonalityAnomalies(series, 14 -> Int.MaxValue)

      anomalies should have size 1
      val (detectionIndex, _) = anomalies.head
      detectionIndex shouldBe 20
    }

    "detect no anomalies on exact linear trend series" in {
      val series = (0 until 48).map(_.toDouble).toVector
      val anomalies = dailyMetricsWithWeeklySeasonalityAnomalies(series, 36 -> Int.MaxValue)
      anomalies shouldBe empty
    }

    "detect no anomalies on exact linear and seasonal effects" in {
      val periodicity = 7
      val series = (0 until 48).map(t => math.sin(2 * math.Pi / periodicity * t))
        .zipWithIndex.map { case (s, level) => s + level }.toVector

      val anomalies = dailyMetricsWithWeeklySeasonalityAnomalies(series, 36 -> Int.MaxValue)
      anomalies shouldBe empty
    }

    "detect anomalies if the training data is wrong" in {
      val train = Vector.fill(2)(Vector[Double](0, 1, 1, 1, 1, 1, 1)).flatten
      val test = Vector[Double](1, 1, 1, 1, 1, 1, 1)
      val series = train ++ test

      val anomalies = dailyMetricsWithWeeklySeasonalityAnomalies(series, 14 -> 21)

      anomalies should have size 1
      val (detectionIndex, _) = anomalies.head
      detectionIndex shouldBe 14
    }

    "work on monthly data with yearly seasonality" in {
      // https://datamarket.com/data/set/22ox/monthly-milk-production-pounds-per-cow-jan-62-dec-75
      val monthlyMilkProduction = Vector[Double](
        589, 561, 640, 656, 727, 697, 640, 599, 568, 577, 553, 582,
        600, 566, 653, 673, 742, 716, 660, 617, 583, 587, 565, 598,
        628, 618, 688, 705, 770, 736, 678, 639, 604, 611, 594, 634,
        658, 622, 709, 722, 782, 756, 702, 653, 615, 621, 602, 635,
        677, 635, 736, 755, 811, 798, 735, 697, 661, 667, 645, 688,
        713, 667, 762, 784, 837, 817, 767, 722, 681, 687, 660, 698,
        717, 696, 775, 796, 858, 826, 783, 740, 701, 706, 677, 711,
        734, 690, 785, 805, 871, 845, 801, 764, 725, 723, 690, 734,
        750, 707, 807, 824, 886, 859, 819, 783, 740, 747, 711, 751,
        804, 756, 860, 878, 942, 913, 869, 834, 790, 800, 763, 800,
        826, 799, 890, 900, 961, 935, 894, 855, 809, 810, 766, 805,
        821, 773, 883, 898, 957, 924, 881, 837, 784, 791, 760, 802,
        828, 778, 889, 902, 969, 947, 908, 867, 815, 812, 773, 813,
        834, 782, 892, 903, 966, 937, 896, 858, 817, 827, 797, 843
      )

      val strategy = new HoltWinters(
        HoltWinters.MetricInterval.Monthly,
        HoltWinters.SeriesSeasonality.Yearly)

      val nYearsTrain = 3
      val nYearsTest = 1
      val trainSize = nYearsTrain * 12
      val testSize = nYearsTest * 12
      val nTotal = trainSize + testSize

      val anomalies = strategy.detect(
        monthlyMilkProduction.take(nTotal),
        trainSize -> nTotal
      )

      anomalies should have size 7
    }

    "work on an additional series with yearly seasonality" in {
      // https://datamarket.com/data/set/22n4/monthly-car-sales-in-quebec-1960-1968
      val monthlyCarSalesQuebec = Vector[Double](
         6550, 8728, 12026, 14395, 14587, 13791, 9498, 8251, 7049, 9545, 9364, 8456,
         7237, 9374, 11837, 13784, 15926, 13821, 11143, 7975, 7610, 10015, 12759, 8816,
        10677, 10947, 15200, 17010, 20900, 16205, 12143, 8997, 5568, 11474, 12256, 10583,
        10862, 10965, 14405, 20379, 20128, 17816, 12268, 8642, 7962, 13932, 15936, 12628,
        12267, 12470, 18944, 21259, 22015, 18581, 15175, 10306, 10792, 14752, 13754, 11738,
        12181, 12965, 19990, 23125, 23541, 21247, 15189, 14767, 10895, 17130, 17697, 16611,
        12674, 12760, 20249, 22135, 20677, 19933, 15388, 15113, 13401, 16135, 17562, 14720,
        12225, 11608, 20985, 19692, 24081, 22114, 14220, 13434, 13598, 17187, 16119, 13713,
        13210, 14251, 20139, 21725, 26099, 21084, 18024, 16722, 14385, 21342, 17180, 14577
      )

      val strategy = new HoltWinters(
        HoltWinters.MetricInterval.Monthly,
        HoltWinters.SeriesSeasonality.Yearly)

      val nYearsTrain = 3
      val nYearsTest = 1
      val trainSize = nYearsTrain * 12
      val testSize = nYearsTest * 12
      val nTotal = trainSize + testSize

      val anomalies = strategy.detect(
        monthlyCarSalesQuebec.take(nTotal),
        trainSize -> nTotal
      )

      anomalies should have size 3
    }
  }
}

object HoltWintersTest {

  def dailyMetricsWithWeeklySeasonalityAnomalies(
    series: Vector[Double], interval: (Int, Int)): Seq[(Int, Anomaly)] = {

    val strategy = new HoltWinters(
      HoltWinters.MetricInterval.Daily,
      HoltWinters.SeriesSeasonality.Weekly
    )

    strategy.detect(series, interval)
  }

}
