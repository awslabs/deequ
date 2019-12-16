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

import com.amazon.deequ.analyzers.KLLParameters
import com.amazon.deequ.examples.ExampleUtils.{itemsAsDataframe, withSpark}
import com.amazon.deequ.profiles.NumericColumnProfile
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

private[examples] object KLLExample extends App {

  withSpark { session =>

    val df = itemsAsDataframe(session,
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12))

    val suggestionResult = ConstraintSuggestionRunner()
      .onData(df)
      .addConstraintRules(Rules.DEFAULT)
      .setKLLParameters(KLLParameters(2, 0.64, 2))
      .run()

    val columnProfiles = suggestionResult.columnProfiles

    println("Observed statistics:")
    columnProfiles.foreach { case (name, profile) =>
      println(s"Feature '$name': ")
      if (profile.dataType.toString == "Integral" || profile.dataType.toString == "Fractional") {
        val numProfile = profile.asInstanceOf[NumericColumnProfile]
        var output =
          s"\tminimum: ${numProfile.minimum.get}\n" +
            s"\tmaximum: ${numProfile.maximum.get}\n" +
            s"\tmean: ${numProfile.mean.get}\n" +
            s"\tstandard deviation: ${numProfile.stdDev.get}\n" +
            s"\tdistribution: {\n" + s"\t\tKLL: {\n"

        output += s"\t\t\tbuckets: [\n"
        val kllMetric = numProfile.kll.get
        kllMetric.buckets.foreach { item =>
          output += s"\t\t\t\t{\n \t\t\t\t\tlow_value: ${item.lowValue} \n " +
            s"\t\t\t\t\thigh_value: ${item.highValue} \n " +
            s"\t\t\t\t\tcount: ${item.count}\n\t\t\t\t}\n"
        }
        output += s"\t\t\t],\n"

        // printing of sketch
        output += s"\t\t\tsketch: {\n"

        // printing of parameters
        output += s"\t\t\t\tparameters: {\n\t\t\t\t\tc: ${kllMetric.parameters(0)}," +
          s"\n\t\t\t\t\tk: ${kllMetric.parameters(1)}\n\t\t\t\t},\n"

        // printing of data
        output += s"\t\t\t\tdata: [\n"
        for (j <- kllMetric.data.indices) {
          val compactor = kllMetric.data(j)
          output += s"\t\t\t\t\t[\n"
          if (j != kllMetric.data.length - 1) {
            for (i <- compactor.indices) {
              if (i == compactor.length - 1) {
                output += s"\t\t\t\t\t\t${compactor(i)}\n"
              } else {
                output += s"\t\t\t\t\t\t${compactor(i)},\n"
              }
            }
            output += s"\t\t\t\t\t],\n"
          } else {
            for (i <- compactor.indices) {
              if (i == compactor.length - 1) {
                output += s"\t\t\t\t\t\t${compactor(i)}\n"
              } else {
                output += s"\t\t\t\t\t\t${compactor(i)},\n"
              }
            }
            output += s"\t\t\t\t\t]\n"
          }
        }
        output += s"\t\t}\n"
        output += s"\t}\n"
        println(output)

      } else {
        profile.histogram.foreach {
          _.values.foreach { case (key, entry) =>
            println(s"\t$key occurred ${entry.absolute} times (ratio is ${entry.ratio})")
          }
        }
      }
    }
  }
}

