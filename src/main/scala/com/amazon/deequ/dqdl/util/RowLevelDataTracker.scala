/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.dqdl.util

import org.apache.spark.sql.Column
import scala.collection.mutable

object RowLevelDataTracker {
  private val columns = new ThreadLocal[mutable.ListBuffer[(String, Column)]] {
    override def initialValue(): mutable.ListBuffer[(String, Column)] = mutable.ListBuffer.empty
  }

  def addColumn(name: String, expression: Column): Unit = columns.get() += ((name, expression))

  def getColumns: Seq[(String, Column)] = columns.get().toSeq

  def reset(): Unit = columns.get().clear()
}
