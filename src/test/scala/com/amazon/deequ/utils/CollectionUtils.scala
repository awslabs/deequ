package com.amazon.deequ.utils

object CollectionUtils {

  implicit class SeqExtensions[A](val source: Seq[A]) {
    def forEachOrder(f: Seq[A] => Any): Unit = {
      source.combinations(source.size)
        .flatMap { _.permutations }
        .foreach { distinctOrder => f(distinctOrder) }
    }

  }
}
