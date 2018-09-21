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

package com.amazon.deequ.suggestions.rules

import com.amazon.deequ.profiles.ColumnProfile
import com.amazon.deequ.suggestions._

/** Abstract base class for all constraint suggestion rules */
abstract class ConstraintRule[P <: ColumnProfile] {

  val ruleDescription: String

  /**
    * Decide whether the rule should be applied to a particular column
    *
    * @param profile  profile of the column
    * @param numRecords overall number of records
    * @return
    */
  def shouldBeApplied(profile: P, numRecords: Long): Boolean

  /**
    * Generated a suggested constraint for the column
    *
    * @param profile  profile of the column
    * @param numRecords overall number of records
    * @return
    */
  def candidate(profile: P, numRecords: Long): ConstraintSuggestion
}
