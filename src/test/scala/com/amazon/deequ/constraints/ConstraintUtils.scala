package com.amazon.deequ.constraints

import org.apache.spark.sql.DataFrame

object ConstraintUtils {

  def calculate(constraint: Constraint, df: DataFrame): ConstraintResult = {

    val analysisBasedConstraint = constraint match {
        case nc: ConstraintDecorator => nc.inner
        case c: Constraint => c
    }

    analysisBasedConstraint.asInstanceOf[AnalysisBasedConstraint[_, _, _]].calculateAndEvaluate(df)
  }
}
