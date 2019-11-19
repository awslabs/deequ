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

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks._


class QuantileNonSample[T](
    var sketchSize: Int,
    var shrinkingFactor: Double = 0.64)
    (implicit ordering: Ordering[T], ct: ClassTag[T])
  extends Serializable {

  /** Current Number of levels in compactors */
  var curNumOfCompactors = 0

  /** Number of items in compactors. */
  var compactorActualSize = 0

  /** Overall capacity of compactors */
  var compactorTotalSize = 0

  /** Initialize the compactors */
  var compactors: ArrayBuffer[NonSampleCompactor[T]] = ArrayBuffer[NonSampleCompactor[T]]()

  expand()

  /** Given sketchSize, shrinkingFactor, and data, reconstruct the KLL Object */
  def reconstruct(
    sketchSize: Int,
    shrinkingFactor: Double,
    data: Array[Array[T]])
  : Unit = {
    this.sketchSize = sketchSize
    this.shrinkingFactor = shrinkingFactor
    compactors = ArrayBuffer.fill(data.length)(new NonSampleCompactor[T])
    for (i <- data.indices) {
      compactors(i).buffer = data(i).to[ArrayBuffer]
    }
    curNumOfCompactors = data.length
    compactorActualSize = getCompactorItemsCount
    compactorTotalSize = getCompactorCapacityCount
  }

  /** Get method for items inside compactors */
  def getCompactorItems: Array[Array[T]] = {
    var ret = ArrayBuffer[Array[T]]()
    compactors.toArray.foreach { compactor =>
      ret = ret :+ compactor.buffer.toArray
    }
    ret.toArray
  }

  /** Expand a layer of compactor */
  def expand(): Unit = {
    compactors = compactors :+ new NonSampleCompactor[T]
    curNumOfCompactors = compactors.length
    compactorTotalSize = getCompactorCapacityCount
  }

  private def capacity(height: Int): Int = {
    2 * (Math.ceil(sketchSize * Math.pow(shrinkingFactor, height) / 2).toInt + 1)
  }

  /**
   * Update the sketch with a single item.
   *
   * @param item new item observed by the sketch
   */
  def update(item: T): Unit = {
    compactors(0).buffer = compactors(0).buffer :+ item
    compactorActualSize = compactorActualSize + 1
    if (compactorActualSize > compactorTotalSize) {
      condense()
    }
  }

  /** Condense the compactors. */
  def condense(): Unit = {
    breakable {
      for (height <- compactors.indices) {
        if (compactors(height).buffer.length >= capacity(height)) {
          if (height + 1 >= curNumOfCompactors) {
            expand()
          }
          val output: Array[T] = compactors(height).compact
          output.foreach {element =>
            compactors(height + 1).buffer = compactors(height + 1).buffer :+ element
          }
          compactorActualSize = getCompactorItemsCount
          break
        }
      }
    }
  }

  private def greaterThan[T](o1: T, o2: T)(implicit ord: Ordering[T]): Boolean = {
    ord.gt(o1, o2)
  }
  private def smallerThan[T](o1: T, o2: T)(implicit ord: Ordering[T]): Boolean = {
    ord.lt(o1, o2)
  }

  /**
   * Get the map which contains the rank of all items which is currently in the sketch.
   *
   * @return the sorted Listmap (by key), which contains the rank of all current items in the sketch
   */
  def getRankMap(): ListMap[T, Long] = {
    val sortedOutput = ListMap(output.toSeq.sortBy({
      case (item, _) => item
    }): _*)
    val states = scala.collection.mutable.Map[T, Long]()
    var runningRank = 0L
    sortedOutput.foreach { case (item, weight) =>
      runningRank = runningRank + weight
      states(item) = runningRank
    }
    ListMap(states.toSeq.sortBy({
      case (item, _) => item
    }): _*)
  }

  /**
   * Get CDF function of sketch items.
   *
   * @return CDF function
   */
  def getCDF(): Array[(T, Double)] = {
    val rankMap = getRankMap()
    val tmp = rankMap.keySet.toArray
    val (_, totalWeight) = rankMap.last
    var ret = ArrayBuffer[(T, Double)]()
    tmp.foreach { item =>
      ret = ret :+ (item, rankMap(item).toDouble / totalWeight.toDouble)
    }
    ret.toArray
  }

  /**
   * Get the rank of query item (inclusive rank) without RankMap.
   *
   * @param item item to query
   * @return the estimated rank of the query item in sketch
   */
  def getRank(item: T): Long = {
    var rank = 0L
    output.foreach {case (target, weight) =>
      if (!greaterThan(target, item)) {
        rank = rank + weight
      }
    }
    rank
  }

  /**
   * Get the rank of query item (exclusive rank) without RankMap.
   * @param item item to query
   * @return the estimated rank of the query item in sketch
   */
  def getRankExclusive(item: T): Long = {
    var rank = 0L
    output.foreach {case (target, weight) =>
      if (smallerThan(target, item)) {
        rank = rank + weight
      }
    }
    rank
  }

  /**
   * Get the rank of query item with RankMap.
   * @param item item to query
   * @param rankMap the estimated rank of the query item in sketch
   * @return
   */
  def getRank(item: T, rankMap: ListMap[T, Long]): Long = {
    var curRank = 0L
    breakable {
      rankMap.foreach { case (target, weight) =>
        if (greaterThan(target, item)) {
          break
        }
        curRank = weight
      }
    }
    curRank
  }



  /**
   * Merge two sketches into a single one.
   *
   * @param that another sketch
   * @return the merged sketch
   */
  def merge(that: QuantileNonSample[T]) : QuantileNonSample[T] = {
    while (this.curNumOfCompactors < that.curNumOfCompactors) {
      this.expand()
    }

    for (i <- 0 until that.curNumOfCompactors) {
      this.compactors(i).buffer = this.compactors(i).buffer ++ that.compactors(i).buffer
    }

    compactorActualSize = getCompactorItemsCount

    while (compactorActualSize >= compactorTotalSize) {
      this.condense()
    }
    this
  }

  private def output: Array[(T, Long)] = {
    val compactorArray = compactors.toArray.slice(0, curNumOfCompactors)
    val compactorIndexMap = compactorArray.zipWithIndex
    compactorIndexMap.flatMap {case (compactor, i) =>
      compactor.buffer.toArray.map((_, 1L << i))
    }
  }

  /**
   * The quantile values of the sketch.
   *
   * @param q number of quantiles required
   * @return quantiles 1/q through (q-1)/q
   */
  def quantiles(q: Int) : Array[T] = {

    if (output.isEmpty) {
      return Array.empty
    }

    val sortedItems = output.sortBy({
      case (item, _) => item
    })
    val totalWeight = sortedItems.map({
      case (_, weight) => weight
    }).sum
    var nextThresh = totalWeight/q
    var curq = 1
    var i = 0
    var sumSoFar: Long = 0
    val (initializedValue, _) = sortedItems(0)
    val quantiles = Array.fill[T](q - 1)(initializedValue)


    while (i < sortedItems.length && curq < q) {
      while (sumSoFar < nextThresh) {
        val (_, weight) = sortedItems(i)
        sumSoFar += weight
        i += 1
      }
      val (item, _) = sortedItems(math.min(i, sortedItems.length - 1))
      quantiles(curq - 1) = item
      curq += 1
      nextThresh = curq * totalWeight / q
    }
    quantiles
  }

  /**
   * Count actual items in compactors.
   *
   * @return number of items existing in compactors
   */
  def getCompactorItemsCount: Int = {
    var size = 0
    compactors.toArray.slice(0, curNumOfCompactors).foreach{ compactor =>
      size = size + compactor.buffer.length
    }
    size
  }

  /**
   * Count total capacity of compactors.
   *
   * @return total capacity of compactors
   */
  def getCompactorCapacityCount: Int = {
    var size = 0
    for (height <- 0 until curNumOfCompactors) {
      size = size + capacity(height)
    }
    size
  }
}

