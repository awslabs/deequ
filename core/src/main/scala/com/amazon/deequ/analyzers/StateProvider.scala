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

package com.amazon.deequ.analyzers

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import com.amazon.deequ.statistics.Statistic

/** Load a stored state for an analyzer */
trait StateLoader {
  def load[S <: State[_]](statistic: Statistic): Option[S]
}

/** Persist a state for an analyzer */
trait StatePersister {
  def persist[S <: State[_]](statistic: Statistic, state: S)
}

/** Store states in memory */
case class InMemoryStateProvider() extends StateLoader with StatePersister {

  private[this] val statesByStatistic = new ConcurrentHashMap[Statistic, State[_]]()

  override def load[S <: State[_]](statistic: Statistic): Option[S] = {
    Option(statesByStatistic.get(statistic).asInstanceOf[S])
  }

  override def persist[S <: State[_]](statistic: Statistic, state: S): Unit = {
    statesByStatistic.put(statistic, state)
  }

  override def toString: String = {
    val buffer = new StringBuilder()
    statesByStatistic.foreach { case (analyzer, state) =>
      buffer.append(analyzer.toString)
      buffer.append(" => ")
      buffer.append(state.toString)
      buffer.append("\n")
    }

    buffer.toString
  }
}