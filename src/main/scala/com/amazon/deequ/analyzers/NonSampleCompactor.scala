package com.amazon.deequ.analyzers

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

/**
 * A quantile sketcher whose output is half the size of its input
 *
 *
 * @tparam T type of the items being sketched. There should an ordering
 *           over this item type
 */
class NonSampleCompactor[T]()
                           (implicit ordering: Ordering[T],
                            ct: ClassTag[T]) extends Serializable {

  var numOfCompress = 0
  var offset = 0
  var buffer: ArrayBuffer[T] = ArrayBuffer[T]()
  private def findOdd(items:Int): Option[T] = (items%2) match {
    case 1 => Some(buffer(math.max(items-1,0)))
    case 0 => None
  }

  def compact : Array[T] = {
    var items = buffer.length
    val len = items - (items % 2)
    if (numOfCompress % 2 == 1) {
      offset = 1 - offset
    }
    // Comment it For unit testing, should uncomment it for real use.
//    else {
//      offset = if (Random.nextBoolean()) 1 else 0
//    }
    val sBuff = buffer.toArray.slice(0,len).sorted
    val output = (offset until len by 2).map(sBuff(_)).toArray
    val tail = findOdd(items)
    items = items % 2
    var newBuffer = ArrayBuffer[T]()
    if (tail.isDefined) newBuffer = newBuffer :+ tail.get
    buffer=newBuffer
    numOfCompress = numOfCompress + 1
    output
  }
}

