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

import org.apache.spark.sql.functions.{col}

private[checks] object ColumnCondition {

  def isEachNotNull(cols: Seq[String]): String = {
    cols
      .map(col(_).isNotNull)
      .reduce(_ and _)
      .toString()
  }

  def isAnyNotNull(cols: Seq[String]): String = {
    cols
      .map(col(_).isNotNull)
      .reduce(_ or _)
      .toString()
  }
}
