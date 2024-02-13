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