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

import ExampleUtils.{itemsAsDataframe, withSpark}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.KLLParameters
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.spark.sql.types.DoubleType

private[examples] object KLLCheckExample extends App {

  withSpark { session =>

    val data = itemsAsDataframe(session,
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12))

    val newData = data.select(data("numViews").cast(DoubleType).as("numViews"))

    val verificationResult = VerificationSuite()
      .onData(newData)
      .addCheck(
        Check(CheckLevel.Error, "integrity checks")
          // we expect 5 records
          .hasSize(_ == 5)
          // we expect the maximum of tips to be not more than 10
          .hasMax("numViews", _ <= 10)
          // we expect the sketch size to be at least 16
          .kllSketchSatisfies("numViews", _.parameters(1) >= 16,
            kllParameters = Option(KLLParameters(2, 0.64, 2))))
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data, the following constraints were not satisfied:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result =>
          println(s"${result.constraint} failed: ${result.message.get}")
        }
    }

  }
}

