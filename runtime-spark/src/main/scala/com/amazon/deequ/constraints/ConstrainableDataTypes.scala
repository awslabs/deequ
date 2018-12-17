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

package com.amazon.deequ.constraints

object ConstrainableDataTypes extends Enumeration {
  val Null: Value = Value(0)
  val Fractional: Value = Value(1)
  val Integral: Value = Value(2)
  val Boolean: Value = Value(3)
  val String: Value = Value(4)
  val Numeric: Value = Value(5) // Union of integral and fractional
}
