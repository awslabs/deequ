/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.dqdl

import com.amazon.deequ.checks.Check
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.DataFrame

/**
 * Executes a sequence of Deequ Checks on a Spark DataFrame.
 */
class DeequCheckExecutor {
  def executeChecks(df: DataFrame, checks: Seq[Check]): VerificationResult = {
    VerificationSuite()
      .onData(df)
      .addChecks(checks)
      .run()
  }
}
