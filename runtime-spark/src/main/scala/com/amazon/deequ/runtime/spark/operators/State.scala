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

package com.amazon.deequ.runtime.spark.operators

/**
 * A state (sufficient statistic) computed from data, from which we can compute a metric.
 * Must be combinable with other states of the same type
 * (= algebraic properties of a commutative semi-group)
 */
trait State[S <: State[S]] {

  // Unfortunately this is required due to type checking issues
  private[analyzers] def sumUntyped(other: State[_]): S = {
    sum(other.asInstanceOf[S])
  }

  /** Combine this with another state */
  def sum(other: S): S

  /** Same as sum, syntatic sugar */
  def +(other: S): S = {
    sum(other)
  }
}

/** A state which produces a double valued metric  */
trait DoubleValuedState[S <: DoubleValuedState[S]] extends State[S] {
  def metricValue(): Double
}