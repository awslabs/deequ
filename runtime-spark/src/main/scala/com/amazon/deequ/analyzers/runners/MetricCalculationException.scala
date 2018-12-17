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

package com.amazon.deequ.analyzers.runners

abstract class MetricCalculationException(message: String) extends Exception(message)

class MetricCalculationRuntimeException(message: String)
  extends MetricCalculationException(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }
}

class MetricCalculationPreconditionException(message: String)
  extends MetricCalculationException(message)


class NoSuchColumnException(message: String)
  extends MetricCalculationPreconditionException(message)

class WrongColumnTypeException(message: String)
  extends MetricCalculationPreconditionException(message)

class NoColumnsSpecifiedException(message: String)
  extends MetricCalculationPreconditionException(message)

class NumberOfSpecifiedColumnsException(message: String)
  extends MetricCalculationPreconditionException(message)

class IllegalAnalyzerParameterException(
    message: String)
  extends MetricCalculationPreconditionException(message)

class EmptyStateException(message: String) extends MetricCalculationRuntimeException(message)


object MetricCalculationException {

  private[deequ] def getApproxQuantileIllegalParamMessage(quantile: Double): String = {
    "Quantile parameter must be in the closed interval [0, 1]. " +
      s"Currently, the value is: $quantile!"
  }

  private[deequ] def getApproxQuantileIllegalErrorParamMessage(relativeError: Double): String = {
    "Relative error parameter must be in the closed interval [0, 1]. " +
      s"Currently, the value is: $relativeError!"
  }

  def wrapIfNecessary(exception: Throwable)
    : MetricCalculationException = {

    exception match {
      case error: MetricCalculationException => error
      case error: Throwable => new MetricCalculationRuntimeException(error)
    }
  }

}
