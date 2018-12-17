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

package com.amazon.deequ.utils

import scala.util.{Failure, Success, Try}

object AssertionUtils {

  implicit class TryUtils[A](something: Try[A]) {
    def compare[B](other: Try[B]): Boolean = {
      (something, other) match {
        case (Success(a), Success(b)) => a == b
        case (Failure(a), Failure(b)) => a.getClass == b.getClass && (a.getMessage == b.getMessage)
        case (_, _) => false
      }
    }
    def compareFailureTypes[B](other: Try[B]): Boolean = {
      (something, other) match {
        case (Failure(a), Failure(b)) => a.getClass == b.getClass
        case (_, _) => false
      }
    }
    def compareOuterAndInnerFailureTypes[B](other: Try[B]): Boolean = {
      (something, other) match {
        case (Failure(a: Throwable), Failure(b: Throwable))
          if (a.getCause != null) && (b.getCause != null) =>
            (a.getClass == b.getClass) && (a.getCause.getClass == b.getCause.getClass)
        case (_, _) => false
      }
    }

  }

}
