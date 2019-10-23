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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

/**
 * A quantile sketcher whose output is half the size of its input.
 *
 * @tparam T type of the items being sketched. There should an ordering
 *           over this item type
 */
class NonSampleCompactor[T]()
     (implicit ordering: Ordering[T],
      ct: ClassTag[T])
  extends Serializable {

  var numOfCompress = 0
  var offset = 0
  var buffer: ArrayBuffer[T] = ArrayBuffer[T]()

  private def findOdd(items: Int): Option[T] = items % 2 match {
    case 1 => Some(buffer(math.max(items - 1, 0)))
    case _ => None
  }

  def compact : Array[T] = {
    var items = buffer.length
    val len = items - (items % 2)
    if (numOfCompress % 2 == 1) {
      offset = 1 - offset
    }
//    else {
//      offset = if (Random.nextBoolean()) 1 else 0
//    }
    val sortedBuffer = buffer.toArray.slice(0, len).sorted

    /** Selects half of the items from this level compactor to the next level compactor.
     * e.g. if sortedBuffer is Array(1,2,3,4), if offset is 1, output = Array(2,4),
     * and if offset is 0, output = Array(1,3), this will be the input to the next level compactor.
     */
    val output = (offset until len by 2).map(sortedBuffer(_)).toArray
    val tail = findOdd(items)
    items = items % 2
    var newBuffer = ArrayBuffer[T]()
    if (tail.isDefined) {
      newBuffer = newBuffer :+ tail.get
    }
    buffer = newBuffer
    numOfCompress = numOfCompress + 1
    output
  }
}

