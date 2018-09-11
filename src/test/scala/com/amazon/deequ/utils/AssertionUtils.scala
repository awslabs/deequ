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
