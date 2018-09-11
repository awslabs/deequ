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

package com.amazon.deequ.checks

import com.amazon.deequ.constraints.Constraint

/** Allows to replace the last configured constraint in a check with a filtered version */
class CheckWithLastConstraintFilterable(
    level: CheckLevel.Value,
    description: String,
    constraints: Seq[Constraint],
    createReplacement: Option[String] => Constraint)
  extends Check(level, description, constraints) {

  /**
    * Defines a filter to apply before evaluating the previous constraint
    *
    * @param filter SparkSQL predicate to apply
    * @return
    */
  def where(filter: String): Check = {

    val adjustedConstraints =
      constraints.take(constraints.size - 1) :+ createReplacement(Option(filter))

    Check(level, description, adjustedConstraints)
  }
}

object CheckWithLastConstraintFilterable {
  def apply(
      level: CheckLevel.Value,
      description: String,
      constraints: Seq[Constraint],
      createReplacement: Option[String] => Constraint
    ): CheckWithLastConstraintFilterable = {

    new CheckWithLastConstraintFilterable(level, description, constraints, createReplacement)
  }
}
