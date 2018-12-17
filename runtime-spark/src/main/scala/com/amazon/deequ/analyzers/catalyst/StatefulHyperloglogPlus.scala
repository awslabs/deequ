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


package org.apache.spark.sql.catalyst.expressions.aggregate

import java.lang.{Long => JLong}
import java.nio.ByteBuffer

import com.amazon.deequ.analyzers.ApproxCountDistinctState
import com.amazon.deequ.analyzers.catalyst.AttributeReferenceCreation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.aggregate.HLLConstants._

/** Adjusted version of org.apache.spark.sql.catalyst.expressions.aggregate.HyperloglogPlus */
private[sql] case class StatefulHyperloglogPlus(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate {

  import DeequHyperLogLogPlusPlusUtils._

  def this(child: Expression) = {
    this(child = child, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)
  }

  override def prettyName: String = "stateful_approx_count_distinct"

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int)
    : ImperativeAggregate = {

    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }


  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = false

  override def dataType: DataType = BinaryType

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  /** Allocate enough words to store all registers. */
  override val aggBufferAttributes: Seq[AttributeReference] = Seq.tabulate(NUM_WORDS) { i =>
    AttributeReferenceCreation.createSafe(s"MS[$i]")
  }
  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override val inputAggBufferAttributes: Seq[AttributeReference] = {
    aggBufferAttributes.map { _.newInstance() }
  }

  /** Fill all words with zeros. */
  override def initialize(buffer: InternalRow): Unit = {
    var word = 0
    while (word < NUM_WORDS) {
      buffer.setLong(mutableAggBufferOffset + word, 0)
      word += 1
    }
  }

  /**
   * Update the HLL++ buffer.
   *
   * Variable names in the HLL++ paper match variable names in the code.
   */
  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val v = child.eval(input)
    if (v != null) {
      // Create the hashed value 'x'.
      val x = XxHash64Function.hash(v, child.dataType, 42L)

      // Determine the index of the register we are going to use.
      val idx = (x >>> IDX_SHIFT).toInt

      // Determine the number of leading zeros in the remaining bits 'w'.
      val pw = JLong.numberOfLeadingZeros((x << P) | W_PADDING) + 1L

      // Get the word containing the register we are interested in.
      val wordOffset = idx / REGISTERS_PER_WORD
      val word = buffer.getLong(mutableAggBufferOffset + wordOffset)

      // Extract the M[J] register value from the word.
      val shift = REGISTER_SIZE * (idx - (wordOffset * REGISTERS_PER_WORD))
      val mask = REGISTER_WORD_MASK << shift
      val Midx = (word & mask) >>> shift

      // Assign the maximum number of leading zeros to the register.
      if (pw > Midx) {
        buffer.setLong(mutableAggBufferOffset + wordOffset, (word & ~mask) | (pw << shift))
      }
    }
  }

  /**
   * Merge the HLL buffers by iterating through the registers in both buffers and select the
   * maximum number of leading zeros for each register.
   */
  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    var idx = 0
    var wordOffset = 0
    while (wordOffset < DeequHyperLogLogPlusPlusUtils.NUM_WORDS) {
      val word1 = buffer1.getLong(mutableAggBufferOffset + wordOffset)
      val word2 = buffer2.getLong(inputAggBufferOffset + wordOffset)
      var word = 0L
      var i = 0
      var mask = REGISTER_WORD_MASK
      while (idx < M && i < REGISTERS_PER_WORD) {
        word |= Math.max(word1 & mask, word2 & mask)
        mask <<= REGISTER_SIZE
        i += 1
        idx += 1
      }
      buffer1.setLong(mutableAggBufferOffset + wordOffset, word)
      wordOffset += 1
    }
  }

  override def eval(buffer: InternalRow): Any = {

    val registers = (0 until NUM_WORDS)
      .map { index => buffer.getLong(mutableAggBufferOffset + index) }
      .toArray

    wordsToBytes(registers)
  }

}

object DeequHyperLogLogPlusPlusUtils {

  val NUM_WORDS = 52
  val RELATIVE_SD = 0.05

  val P: Int = Math.ceil(2.0d * Math.log(1.106d / RELATIVE_SD) / Math.log(2.0d)).toInt

  val IDX_SHIFT: Int = JLong.SIZE - P
  val W_PADDING: Long = 1L << (P - 1)
  val M: Int = 1 << P

  private[this] val ALPHA_M2 = P match {
    case 4 => 0.673d * M * M
    case 5 => 0.697d * M * M
    case 6 => 0.709d * M * M
    case _ => (0.7213d / (1.0d + 1.079d / M)) * M * M
  }

  def wordsToBytes(words: Array[Long]): Array[Byte] = {
    require(words.length == NUM_WORDS)
    val bytes = Array.ofDim[Byte](NUM_WORDS * JLong.SIZE)
    val buffer = ByteBuffer.wrap(bytes).asLongBuffer()

    words.foreach { word => buffer.put(word) }

    bytes
  }

  def wordsFromBytes(bytes: Array[Byte]): ApproxCountDistinctState = {
    require(bytes.length == NUM_WORDS * JLong.SIZE)
    val buffer = ByteBuffer.wrap(bytes).asLongBuffer().asReadOnlyBuffer()
    val words = Array.fill[Long](NUM_WORDS) { buffer.get() }

    ApproxCountDistinctState(words)
  }

  def merge(words1: Array[Long], words2: Array[Long]): Array[Long] = {
    val dest = Array.ofDim[Long](NUM_WORDS)
    var idx = 0
    var wordOffset = 0
    while (wordOffset < NUM_WORDS) {
      val word1 = words1(wordOffset)
      val word2 = words2(wordOffset)
      var word = 0L
      var i = 0
      var mask = REGISTER_WORD_MASK
      while (idx < M && i < REGISTERS_PER_WORD) {
        word |= Math.max(word1 & mask, word2 & mask)
        mask <<= REGISTER_SIZE
        i += 1
        idx += 1
      }
      dest(wordOffset) = word
      wordOffset += 1
    }
    dest
  }

  def count(words: Array[Long]): Double = {
    // Compute the inverse of indicator value 'z' and count the number of zeros 'V'.
    var zInverse = 0.0d
    var V = 0.0d
    var idx = 0
    var wordOffset = 0
    while (wordOffset < words.length) {
      val word = words(wordOffset)
      var i = 0
      var shift = 0
      while (idx < M && i < REGISTERS_PER_WORD) {
        val Midx = (word >>> shift) & REGISTER_WORD_MASK
        zInverse += 1.0 / (1 << Midx)
        if (Midx == 0) {
          V += 1.0d
        }
        shift += REGISTER_SIZE
        i += 1
        idx += 1
      }
      wordOffset += 1
    }

    // We integrate two steps from the paper:
    // val Z = 1.0d / zInverse
    // val E = alphaM2 * Z
    @inline
    def EBiasCorrected = ALPHA_M2 / zInverse match {
      case e if P < 19 && e < 5.0d * M => e - estimateBias(e)
      case e => e
    }

    // Estimate the cardinality.
    val estimate = if (V > 0) {
      // Use linear counting for small cardinality estimates.
      val H = M * Math.log(M / V)
      if (H <= THRESHOLDS(P - 4)) {
        H
      } else {
        EBiasCorrected
      }
    } else {
      EBiasCorrected
    }

    // Round to the nearest long value.
    Math.round(estimate)
  }

  def estimateBias(e: Double): Double = {
    val estimates = RAW_ESTIMATE_DATA(P - 4)
    val numEstimates = estimates.length

    // The estimates are sorted so we can use a binary search to find the index of the
    // interpolation estimate closest to the current estimate.
    val nearestEstimateIndex = java.util.Arrays.binarySearch(estimates, 0, numEstimates, e) match {
      case ix if ix < 0 => -(ix + 1)
      case ix => ix
    }

    // Use square of the difference between the current estimate and the estimate at the given
    // index as distance metric.
    def distance(i: Int): Double = {
      val diff = e - estimates(i)
      diff * diff
    }

    // Keep moving bounds as long as the (exclusive) high bound is closer to the estimate than
    // the lower (inclusive) bound.
    var low = math.max(nearestEstimateIndex - K + 1, 0)
    var high = math.min(low + K, numEstimates)
    while (high < numEstimates && distance(high) < distance(low)) {
      low += 1
      high += 1
    }

    // Calculate the sum of the biases in low-high interval.
    val biases = BIAS_DATA(P - 4)
    var i = low
    var biasSum = 0.0
    while (i < high) {
      biasSum += biases(i)
      i += 1
    }

    // Calculate the bias.
    biasSum / (high - low)
  }
}
