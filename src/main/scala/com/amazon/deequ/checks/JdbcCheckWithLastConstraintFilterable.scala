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

import com.amazon.deequ.constraints.JdbcConstraint


/** Allows to replace the last configured constraint in a check with a filtered version */
class JdbcCheckWithLastConstraintFilterable(
                                         level: JdbcCheckLevel.Value,
                                         description: String,
                                         constraints: Seq[JdbcConstraint],
                                         createReplacement: Option[String] => JdbcConstraint)
  extends JdbcCheck(level, description, constraints) {

  /**
    * Defines a filter to apply before evaluating the previous constraint
    *
    * @param filter SparkSQL predicate to apply
    * @return
    */
  def where(filter: String): JdbcCheck = {

    val adjustedConstraints =
      constraints.take(constraints.size - 1) :+ createReplacement(Option(filter))

    JdbcCheck(level, description, adjustedConstraints)
  }
}

object JdbcCheckWithLastConstraintFilterable {
  def apply(
             level: JdbcCheckLevel.Value,
             description: String,
             constraints: Seq[JdbcConstraint],
             createReplacement: Option[String] => JdbcConstraint
           ): JdbcCheckWithLastConstraintFilterable = {

    new JdbcCheckWithLastConstraintFilterable(level, description, constraints, createReplacement)
  }
}
