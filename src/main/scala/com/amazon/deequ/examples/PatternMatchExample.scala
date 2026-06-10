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

import ExampleUtils.{withSpark, itemsAsDataframe}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.analyzers.{PatternMatch, Patterns}

private[examples] object PatternMatchExample extends App {

  withSpark { session =>

    // Create sample data with SSN, PostalCode, and PhoneNumber columns
    val data = itemsAsDataframe(session,
      Item(1, "123-45-6789", "12345", "+1 234-567-8901"),         // Valid SSN, Postal Code, and Phone Number
      Item(2, "078-05-1120", "12345-6789", "(234) 567-8901 x1234"), // Valid SSN, Postal Code, and Phone Number with extension
      Item(3, "000-12-3456", "54321", "234-567-8901"),             // Invalid SSN, valid Postal Code and Phone Number
      Item(4, "666-45-6789", "ABCDE", "+1 234 567 8901"),          // Invalid SSN, invalid Postal Code, valid Phone Number
      Item(5, "123-00-6789", "54321", "123-45-6789")               // Invalid SSN, valid Postal Code, invalid Phone Number (formatted like SSN)
    )

    // Run the verification suite with pattern match checks using PatternMatch analyzers
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "pattern match checks")
          // SSN should match the US Social Security Number pattern
          .containsPattern("SSN", Patterns.SOCIAL_SECURITY_NUMBER_US)
          // PostalCode should match the US Postal Code pattern
          .containsPattern("PostalCode", Patterns.POSTAL_CODE_US)
          // PhoneNumber should match the US Phone Number pattern
          .containsPattern("PhoneNumber", Patterns.PHONE_NUMBER_US))
      .run()

    // Check the verification results
    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, all patterns matched!")
    } else {
      println("We found errors in the data, the following pattern checks were not satisfied:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result =>
          println(s"${result.constraint} failed: ${result.message.get}")
        }
    }

  }

  // Define the item case class to match the schema used in the DataFrame
  case class Item(id: Int, SSN: String, PostalCode: String, PhoneNumber: String)
}