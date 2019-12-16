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

package com.amazon.deequ.analyzers.catalyst

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import com.amazon.deequ.analyzers.{NonSampleCompactor, QuantileNonSample}
import com.google.common.primitives.{Doubles, Ints}

class KLLSketchSerializer {

    private final def compactorLength(compactor: NonSampleCompactor[Double]): Int = {
      // compactor.numOfCompress, compactor.offset
      Ints.BYTES + Ints.BYTES +
        // compactor.buffer.length
        Ints.BYTES +
        // compactor buffer array
        compactor.buffer.length * Doubles.BYTES
    }

    private final def getCompactorLengthInSample (sketch: QuantileNonSample[Double]): Int = {
      val tmp = sketch.compactors
      val len = tmp.length
      var count = 0
      for (i <- 0 until len) {
        count += compactorLength(tmp(i))
      }
      count
    }

    private final def length(sketch: QuantileNonSample[Double]): Int = {
      // sketch.sketchSize, sketch.shrinkingFactor
      Ints.BYTES + Doubles.BYTES +
        // sketch.curNumOfCompactors, sketch.compactorActualSize, sketch.compactorTotalSize
        Ints.BYTES + Ints.BYTES + Ints.BYTES +
        // sketch.compactors length
        Ints.BYTES +
        // sketch.compactors
        getCompactorLengthInSample(sketch)
    }

    final def serialize(obj: QuantileNonSample[Double]): Array[Byte] = {

      val buffer = ByteBuffer.wrap(new Array(length(obj)))
      buffer.putInt(obj.sketchSize)
      buffer.putDouble(obj.shrinkingFactor)
      buffer.putInt(obj.curNumOfCompactors)
      buffer.putInt(obj.compactorActualSize)
      buffer.putInt(obj.compactorTotalSize)
      val compactors = obj.compactors
      buffer.putInt(compactors.length)

      var j = 0
      while ( j < compactors.length) {
        val compactor = compactors(j)
        buffer.putInt(compactor.numOfCompress)
        buffer.putInt(compactor.offset)
        buffer.putInt(compactor.buffer.length)
        for (y <- compactor.buffer.indices) {
          buffer.putDouble(compactor.buffer(y))
        }
        j = j + 1
      }
      buffer.array()
    }

    final def deserialize(bytes: Array[Byte]): QuantileNonSample[Double] = {
      val buffer = ByteBuffer.wrap(bytes)
      val sketchSize = buffer.getInt()
      val shrinkingFactor = buffer.getDouble()
      val curNumOfCompactors = buffer.getInt()
      val compactorActualSize = buffer.getInt()
      val compactorTotalSize = buffer.getInt()
      val compactorLength = buffer.getInt()

      // reconstruct compactors
      var compactors = ArrayBuffer[NonSampleCompactor[Double]]()
      var i = 0
      while (i < compactorLength) {
        var compactor = new NonSampleCompactor[Double]()
        val numOfCompress = buffer.getInt()
        val offset = buffer.getInt()
        val bufferLength = buffer.getInt()
        compactor.numOfCompress = numOfCompress
        compactor.offset = offset
        for (_ <- 0 until bufferLength) {
          compactor.buffer = compactor.buffer :+ buffer.getDouble()
        }
        compactors = compactors :+ compactor
        i += 1
      }

      var ret = new QuantileNonSample[Double](sketchSize, shrinkingFactor)
      ret.curNumOfCompactors = curNumOfCompactors
      ret.compactorActualSize = compactorActualSize
      ret.compactorTotalSize = compactorTotalSize
      ret.compactors = compactors
      ret
    }
}

object KLLSketchSerializer{
  val serializer: KLLSketchSerializer = new KLLSketchSerializer
}

