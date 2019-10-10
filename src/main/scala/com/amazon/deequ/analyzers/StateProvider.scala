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

import java.util.concurrent.ConcurrentHashMap

import com.google.common.io.Closeables
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{ApproximatePercentile, DeequHyperLogLogPlusPlusUtils}
import org.apache.spark.sql.SaveMode

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

private object StateInformation {
  // 4 byte for the id of an DataTypeInstances entry + 8 byte for the (Long) count of that data type
  // as used in HdfsStateProvider.persistDataTypeState and HdfsStateProvider.loadDataTypeState
  final val dataTypeCountEntrySizeInBytes = 4 + 8
}

/** Load a stored state for an analyzer */
trait StateLoader {
  def load[S <: State[_]](analyzer: Analyzer[S, _]): Option[S]
}

/** Persist a state for an analyzer */
trait StatePersister {
  def persist[S <: State[_]](analyzer: Analyzer[S, _], state: S)
}

/** Store states in memory */
case class InMemoryStateProvider() extends StateLoader with StatePersister {

  private[this] val statesByAnalyzer = new ConcurrentHashMap[Analyzer[_, _], State[_]]()

  override def load[S <: State[_]](analyzer: Analyzer[S, _]): Option[S] = {
    Option(statesByAnalyzer.get(analyzer).asInstanceOf[S])
  }

  override def persist[S <: State[_]](analyzer: Analyzer[S, _], state: S): Unit = {
    statesByAnalyzer.put(analyzer, state)
  }

  override def toString: String = {
    val buffer = new StringBuilder()
    statesByAnalyzer.foreach { case (analyzer, state) =>
      buffer.append(analyzer.toString)
      buffer.append(" => ")
      buffer.append(state.toString)
      buffer.append("\n")
    }

    buffer.toString
  }
}

/** Store states on a filesystem (supports local disk, HDFS, S3) */
case class HdfsStateProvider(
    session: SparkSession,
    locationPrefix: String,
    numPartitionsForHistogram: Int = 10,
    allowOverwrite: Boolean = false)
  extends StateLoader with StatePersister {

  import com.amazon.deequ.io.DfsUtils._

  private[this] def toIdentifier[S <: State[_]](analyzer: Analyzer[S, _]): String = {
    MurmurHash3.stringHash(analyzer.toString, 42).toString
  }

  override def persist[S <: State[_]](analyzer: Analyzer[S, _], state: S): Unit = {

    val identifier = toIdentifier(analyzer)

    analyzer match {
      case _: Size =>
        persistLongState(state.asInstanceOf[NumMatches].numMatches, identifier)

      case _ : Completeness | _ : Compliance | _ : PatternMatch =>
        persistLongLongState(state.asInstanceOf[NumMatchesAndCount], identifier)

      case _: Sum =>
        persistDoubleState(state.asInstanceOf[SumState].sum, identifier)

      case _: Mean =>
        persistDoubleLongState(state.asInstanceOf[MeanState], identifier)

      case _: Minimum =>
        persistDoubleState(state.asInstanceOf[MinState].minValue, identifier)

      case _: Maximum =>
        persistDoubleState(state.asInstanceOf[MaxState].maxValue, identifier)

      case _: MaxLength =>
        persistDoubleState(state.asInstanceOf[MaxState].maxValue, identifier)

      case _: MinLength =>
        persistDoubleState(state.asInstanceOf[MinState].minValue, identifier)

      case _ : FrequencyBasedAnalyzer | _ : Histogram =>
        persistDataframeLongState(state.asInstanceOf[FrequenciesAndNumRows], identifier)

      case _: DataType =>
        val histogram = state.asInstanceOf[DataTypeHistogram]
        persistBytes(DataTypeHistogram.toBytes(histogram.numNull, histogram.numFractional,
          histogram.numIntegral, histogram.numBoolean, histogram.numString), identifier)

      case _: ApproxCountDistinct =>
        val counters = state.asInstanceOf[ApproxCountDistinctState]
        persistBytes(DeequHyperLogLogPlusPlusUtils.wordsToBytes(counters.words), identifier)

      case _ : Correlation =>
        persistCorrelationState(state.asInstanceOf[CorrelationState], identifier)

      case _ : StandardDeviation =>
        persistStandardDeviationState(state.asInstanceOf[StandardDeviationState], identifier)

      case _: ApproxQuantile =>
        val percentileDigest = state.asInstanceOf[ApproxQuantileState].percentileDigest
        val serializedDigest = ApproximatePercentile.serializer.serialize(percentileDigest)
        persistBytes(serializedDigest, identifier)

      case _ =>
        throw new IllegalArgumentException(s"Unable to persist state for analyzer $analyzer.")
    }
  }

  override def load[S <: State[_]](analyzer: Analyzer[S, _]): Option[S] = {

    val identifier = toIdentifier(analyzer)

    val state: Any = analyzer match {

      case _ : Size => NumMatches(loadLongState(identifier))

      case _ : Completeness | _ : Compliance | _ : PatternMatch => loadLongLongState(identifier)

      case _ : Sum => SumState(loadDoubleState(identifier))

      case _ : Mean => loadDoubleLongState(identifier)

      case _ : Minimum => MinState(loadDoubleState(identifier))

      case _ : Maximum => MaxState(loadDoubleState(identifier))

      case _ : MaxLength => MaxState(loadDoubleState(identifier))

      case _ : MinLength => MinState(loadDoubleState(identifier))

      case _ : FrequencyBasedAnalyzer | _ : Histogram => loadDataframeLongState(identifier)

      case _ : DataType => DataTypeHistogram.fromBytes(loadBytes(identifier))

      case _ : ApproxCountDistinct =>
        DeequHyperLogLogPlusPlusUtils.wordsFromBytes(loadBytes(identifier))

      case _ : Correlation => loadCorrelationState(identifier)

      case _ : StandardDeviation => loadStandardDeviationState(identifier)

       case _: ApproxQuantile =>
         val percentileDigest = ApproximatePercentile.serializer.deserialize(loadBytes(identifier))
         ApproxQuantileState(percentileDigest)

      case _ =>
        throw new IllegalArgumentException(s"Unable to load state for analyzer $analyzer.")
    }

    Option(state.asInstanceOf[S])
  }

  private[this] def persistLongState(state: Long, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) {
      _.writeLong(state)
    }
  }

  private[this] def persistDoubleState(state: Double, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) {
      _.writeDouble(state)
    }
  }

  private[this] def persistLongLongState(state: NumMatchesAndCount, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeLong(state.numMatches)
      out.writeLong(state.count)
    }
  }

  private[this] def persistDoubleLongState(state: MeanState, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.sum)
      out.writeLong(state.count)
    }
  }

  private[this] def persistBytes(bytes: Array[Byte], identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeInt(bytes.length)
      for (index <- bytes.indices) {
        out.writeByte(bytes(index))
      }
    }
  }

  private[this] def persistDataframeLongState(
      state: FrequenciesAndNumRows,
      identifier: String)
    : Unit = {

    val saveMode = if (allowOverwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.ErrorIfExists
    }

    state.frequencies
      .coalesce(numPartitionsForHistogram)
      .write.mode(saveMode).parquet(s"$locationPrefix-$identifier-frequencies.pqt")

    writeToFileOnDfs(session, s"$locationPrefix-$identifier-num_rows.bin", allowOverwrite) {
      _.writeLong(state.numRows)
    }
  }

  private[this] def persistCorrelationState(state: CorrelationState, identifier: String) {
    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.n)
      out.writeDouble(state.xAvg)
      out.writeDouble(state.yAvg)
      out.writeDouble(state.ck)
      out.writeDouble(state.xMk)
      out.writeDouble(state.yMk)
    }
  }

  private[this] def persistStandardDeviationState(
      state: StandardDeviationState,
      identifier: String) {

    writeToFileOnDfs(session, s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.n)
      out.writeDouble(state.avg)
      out.writeDouble(state.m2)
    }
  }

  private[this] def loadLongState(identifier: String): Long = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { _.readLong() }
  }

  private[this] def loadDoubleState(identifier: String): Double = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { _.readDouble() }
  }

  private[this] def loadLongLongState(identifier: String): NumMatchesAndCount = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
      NumMatchesAndCount(in.readLong(), in.readLong())
    }
  }

  private[this] def loadDoubleLongState(identifier: String): MeanState = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
      MeanState(in.readDouble(), in.readLong())
    }
  }

  private[this] def loadBytes(identifier: String): Array[Byte] = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
      val length = in.readInt()
      Array.fill(length) { in.readByte() }
    }
  }

  private[this] def loadDataframeLongState(identifier: String): FrequenciesAndNumRows = {
    val frequencies = session.read.parquet(s"$locationPrefix-$identifier-frequencies.pqt")
    val numRows = readFromFileOnDfs(session, s"$locationPrefix-$identifier-num_rows.bin") {
      _.readLong()
    }

    FrequenciesAndNumRows(frequencies, numRows)
  }

  private[this] def loadCorrelationState(identifier: String): CorrelationState = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
    CorrelationState(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(),
      in.readDouble(), in.readDouble())
    }
  }

  private[this] def loadStandardDeviationState(identifier: String): StandardDeviationState = {
    readFromFileOnDfs(session, s"$locationPrefix-$identifier.bin") { in =>
      StandardDeviationState(in.readDouble(), in.readDouble(), in.readDouble())
    }
  }
}
