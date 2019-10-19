package com.amazon.deequ.analyzers

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks._


class QuantileNonSample[T](val sketchSize: Int,
                           val shrinkingFactor: Double = 0.64)
                          (implicit ordering: Ordering[T],
                           ct: ClassTag[T]) extends Serializable{

  private var curNumOfCompactors = 0
  // number of items in compactors
  private var compactorActualSize = 0
  // overall capacity of compactors
  private var compactorTotalSize = 0
  // initialize with ArrayBuffer, add compactors later
  private var compactors = ArrayBuffer[NonSampleCompactor[T]]()
  expand()

  // get and set method for the serialize/deserialize functions
  def getShrinkingFactor: Double = {
    shrinkingFactor
  }

  def getSketchSize: Int = {
    sketchSize
  }

  def getCurNumOfCompactors: Int = {
    curNumOfCompactors
  }

  def getCompactorActualSize: Int = {
    compactorActualSize
  }

  def getCompactorTotalSize: Int = {
    compactorTotalSize
  }

  def getCompactorItems: Array[Array[T]] = {
    var ret = ArrayBuffer[Array[T]]()
    compactors.toArray.foreach {compactor =>
      ret = ret :+ compactor.buffer.toArray
    }
    ret.toArray
  }

  def getCompactor: ArrayBuffer[NonSampleCompactor[T]] = {
    compactors
  }


  // TODO: set stricter access modifier here
  def setCurNumOfCompactors(input : Int): Unit = {
    curNumOfCompactors = input
  }

  def setCompactorActualSize(input : Int): Unit = {
    compactorActualSize = input
  }

  def setCompactorTotalSize(input : Int): Unit = {
    compactorTotalSize = input
  }

  def setCompactor(input: ArrayBuffer[NonSampleCompactor[T]]) : Unit = {
    compactors = input
  }



  // expand a layer of compactor
  def expand(): Unit = {
    compactors = compactors :+ new NonSampleCompactor[T]
    curNumOfCompactors = compactors.length
    compactorTotalSize = getCompactorCapacityCount
  }

  private def capacity(height:Int): Int = {
    2 * (Math.ceil(sketchSize * Math.pow(shrinkingFactor, height) / 2).toInt + 1)
  }

  /**
   * update the sketch with a single item
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

  def condense(): Unit = {
    breakable {
      for (height <- compactors.indices) {
        if (compactors(height).buffer.length >= capacity(height)) {
          if (height + 1 >= curNumOfCompactors) expand()
          val output: Array[T] = compactors(height).compact
          output.foreach(element => compactors(height + 1).buffer = compactors(height + 1).buffer :+ element)
          compactorActualSize = getCompactorItemsCount
          break
        }
      }
    }
  }

  private def compareGreater[T](o1: T, o2: T)(implicit ord: Ordering[T]): Boolean = ord.gt(o1, o2)
  private def compareSmaller[T](o1: T, o2: T)(implicit ord: Ordering[T]): Boolean = ord.lt(o1,o2)

  /**
   * Get the map which contains the rank of all items which is currently in the sketch.
   * @return the sorted Listmap (by key)which contains the rank of all items which is currently in the sketch (RankMap)
   */
  def getRankMap(): ListMap[T,Long] = {
    val sortedOutput = ListMap(output.toSeq.sortBy(_._1):_*)
    var states = scala.collection.mutable.Map[T,Long]()
    var runningRank = 0L
    sortedOutput.foreach{ tuple =>
      runningRank = runningRank + tuple._2
      states(tuple._1) = runningRank
    }
    ListMap(states.toSeq.sortBy(_._1):_*)
  }

  /**
   * Get CDF function of sketch items.
   * @return CDF function
   */
  def getCDF(): Array[(T,Double)] = {
    val rankMap = getRankMap()
    val tmp = rankMap.keySet.toArray
    val totalWeight = rankMap.last._2
    var ret = ArrayBuffer[(T,Double)]()
    tmp.foreach{ item =>
      ret = ret :+ (item, rankMap(item).toDouble / totalWeight.toDouble)
    }
    ret.toArray
  }

  /**
   * Get the rank of query item (inclusive rank) without RankMap.
   * @param item item to query
   * @return the estimated rank of the query item in sketch
   */
  def getRank(item:T): Long = {
    var r = 0L
    output.foreach(tuple => {
      if (!compareGreater(tuple._1,item)) {
        r = r + tuple._2
      }
    })
    r
  }

  /**
   * Get the rank of query item (exclusive rank) without RankMap.
   * @param item item to query
   * @return the estimated rank of the query item in sketch
   */
  def getRankExclusive(item:T): Long = {
    var r = 0L
    output.foreach(tuple => {
      if (compareSmaller(tuple._1,item)) {
        r = r + tuple._2
      }
    })
    r
  }

  /**
   * Get the rank of query item with RankMap.
   * @param item item to query
   * @param rankMap the estimated rank of the query item in sketch
   * @return
   */
  def getRank(item:T, rankMap:ListMap[T,Long]): Long = {
    var curRank = 0L
    breakable {
      for (target <- rankMap) {
        if (compareGreater(target._1, item))
          break
        curRank = target._2
      }
    }
    curRank
  }



  /**
   * merge two sketches into a single one
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

  private def output: Array[(T,Long)] = {
    compactors.toArray.slice(0,curNumOfCompactors).zipWithIndex.flatMap{case (compactor,i) =>
      compactor.buffer.toArray.map((_,1L << i))
    }
  }

  /**
   * the quantile values of the sketch
   *
   * @param q number of quantiles required
   * @return quantiles 1/q through (q-1)/q
   */
  def quantiles(q: Int) : Array[T] = {
    val sortedItems = output.sortBy(_._1)
    val size = sortedItems.map(_._2).sum
    var nextThresh = size/q
    var curq = 1
    var i=0
    var sumSoFar:Long=0
    val quantiles = Array.fill[T](q-1)(sortedItems(0)._1)


    while (i<sortedItems.length && curq<q) {
      while (sumSoFar < nextThresh) {
        sumSoFar += sortedItems(i)._2
        i+=1
      }
      quantiles(curq -1) = sortedItems(math.min(i,sortedItems.length-1))._1
      curq +=1
      nextThresh = curq * size / q
    }
    quantiles
  }

  /**
   * Count actual items in compactors.
   * @return number of items existing in compactors
   */
  def getCompactorItemsCount: Int = {
    var size = 0
    compactors.toArray.slice(0,curNumOfCompactors).foreach{ compactor=>
      size = size + compactor.buffer.length
    }
    size
  }

  /**
   * Count total capacity of compactors.
   * @return total capacity of compactors
   */
  def getCompactorCapacityCount: Int = {
    var size = 0
    for (height <- 0 until curNumOfCompactors) {
      size = size + capacity(height)
    }
    size
  }

  private def getCompactorBuffer: Array[Int] = {
    var size = ArrayBuffer[Int]()
    compactors.toArray.slice(0,curNumOfCompactors).foreach{ compactor=>
      size = size :+ compactor.buffer.length
    }
    size.toArray
  }

  private def getCompactorCapacity: Array[Int] = {
    var size = Array[Int]()
    for (height <- 0 until curNumOfCompactors) {
      size = size :+ capacity(height)
    }
    size
  }

  def printCompactorItems() : Unit = {
    compactors.foreach{ compactor=>
      print("[" )
      var count = 0
      compactor.buffer.foreach{item=>
        print(item)
        count = count + 1
        if (count != compactor.buffer.length) print(",")
      }
      println("]")
    }
  }
}

