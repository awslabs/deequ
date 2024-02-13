/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.utilities
import com.amazon.deequ.utilities.FilteredRow.FilteredRow

/**
 * Trait that defines how row level results will be treated when a filter is applied to an analyzer
 */
trait RowLevelFilterTreatment {
  def rowLevelFilterTreatment: FilteredRow
}

/**
 * Companion object for RowLevelFilterTreatment
 * Defines a sharedInstance that can be used throughout the VerificationRunBuilder
 */
object RowLevelFilterTreatment {
  private var _sharedInstance: RowLevelFilterTreatment = new RowLevelFilterTreatmentImpl(FilteredRow.TRUE)

  def sharedInstance: RowLevelFilterTreatment = _sharedInstance

  def setSharedInstance(instance: RowLevelFilterTreatment): Unit = {
    _sharedInstance = instance
  }
}

class RowLevelFilterTreatmentImpl(initialFilterTreatment: FilteredRow) extends RowLevelFilterTreatment {
  override val rowLevelFilterTreatment: FilteredRow = initialFilterTreatment
}

object FilteredRow extends Enumeration {
  type FilteredRow = Value
  val NULL, TRUE = Value
}

trait RowLevelAnalyzer extends RowLevelFilterTreatment {
  def rowLevelFilterTreatment: FilteredRow.Value = RowLevelFilterTreatment.sharedInstance.rowLevelFilterTreatment
}
