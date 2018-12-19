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

package com.amazon.deequ.analyzers.jdbc

import java.util.concurrent.ConcurrentHashMap

import com.amazon.deequ.analyzers._

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

import com.amazon.deequ.io.LocalDiskUtils

private object StateInformation {
  // 4 byte for the id of an DataTypeInstances entry + 8 byte for the (Long) count of that data type
  // as used in HdfsStateProvider.persistDataTypeState and HdfsStateProvider.loadDataTypeState
  final val dataTypeCountEntrySizeInBytes = 4 + 8
}

/** Load a stored state for an analyzer */
trait JdbcStateLoader {
  def load[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): Option[S]
}

/** Persist a state for an analyzer */
trait JdbcStatePersister {
  def persist[S <: State[_]](analyzer: JdbcAnalyzer[S, _], state: S)
}

/** Store states in memory */
case class JdbcInMemoryStateProvider() extends JdbcStateLoader with JdbcStatePersister {

  private[this] val statesByAnalyzer = new ConcurrentHashMap[JdbcAnalyzer[_, _], State[_]]()

  override def load[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): Option[S] = {
    Option(statesByAnalyzer.get(analyzer).asInstanceOf[S])
  }

  override def persist[S <: State[_]](analyzer: JdbcAnalyzer[S, _], state: S): Unit = {
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

/** Store states on a filesystem (supports local disk) */
case class JdbcFileSystemStateProvider(
    locationPrefix: String,
    numPartitionsForHistogram: Int = 10,
    allowOverwrite: Boolean = false)
  extends JdbcStateLoader with JdbcStatePersister {

  private[this] def toIdentifier[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): String = {
    MurmurHash3.stringHash(analyzer.toString, 42).toString
  }

  override def persist[S <: State[_]](analyzer: JdbcAnalyzer[S, _], state: S): Unit = {

    val identifier = toIdentifier(analyzer)

    analyzer match {
      case _: JdbcSize =>
        persistLongState(state.asInstanceOf[NumMatches].numMatches, identifier)

      case _ : JdbcCompleteness | _ : JdbcCompliance =>
        persistLongLongState(state.asInstanceOf[NumMatchesAndCount], identifier)

      case _: JdbcSum =>
        persistDoubleState(state.asInstanceOf[SumState].sum, identifier)

      case _: JdbcMean =>
        persistDoubleLongState(state.asInstanceOf[MeanState], identifier)

      case _: JdbcMinimum =>
        persistDoubleState(state.asInstanceOf[MinState].minValue, identifier)

      case _: JdbcMaximum =>
        persistDoubleState(state.asInstanceOf[MaxState].maxValue, identifier)

      case _: JdbcHistogram =>
        persistFrequenciesLongState(state.asInstanceOf[JdbcFrequenciesAndNumRows], identifier)

      case _: JdbcEntropy =>
        persistFrequenciesLongState(state.asInstanceOf[JdbcFrequenciesAndNumRows], identifier)

      case _ : JdbcStandardDeviation =>
        persistStandardDeviationState(state.asInstanceOf[StandardDeviationState], identifier)

      case _ : JdbcCorrelation =>
        persistCorrelationState(state.asInstanceOf[CorrelationState], identifier)

      case _ : JdbcUniqueness =>
        persistFrequenciesLongState(state.asInstanceOf[JdbcFrequenciesAndNumRows], identifier)

      case _ =>
        throw new IllegalArgumentException(s"Unable to persist state for analyzer $analyzer.")
    }
  }

  override def load[S <: State[_]](analyzer: JdbcAnalyzer[S, _]): Option[S] = {

    val identifier = toIdentifier(analyzer)

    val state: Any = analyzer match {

      case _ : JdbcSize => NumMatches(loadLongState(identifier))

      case _ : JdbcCompleteness | _ : JdbcCompliance => loadLongLongState(identifier)

      case _ : JdbcSum => SumState(loadDoubleState(identifier))

      case _ : JdbcMean => loadDoubleLongState(identifier)

      case _ : JdbcMinimum => MinState(loadDoubleState(identifier))

      case _ : JdbcMaximum => MaxState(loadDoubleState(identifier))

      case _ : JdbcHistogram => loadFrequenciesLongState(identifier)

      case _ : JdbcEntropy => loadFrequenciesLongState(identifier)

      case _ : JdbcStandardDeviation => loadStandardDeviationState(identifier)

      case _ : JdbcCorrelation => loadCorrelationState(identifier)

      case _ : JdbcUniqueness => loadFrequenciesLongState(identifier)

      case _ =>
        throw new IllegalArgumentException(s"Unable to load state for analyzer $analyzer.")
    }

    Option(state.asInstanceOf[S])
  }

  private[this] def persistLongState(state: Long, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) {
      _.writeLong(state)
    }
  }

  private[this] def persistDoubleState(state: Double, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) {
      _.writeDouble(state)
    }
  }

  private[this] def persistLongLongState(state: NumMatchesAndCount, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeLong(state.numMatches)
      out.writeLong(state.count)
    }
  }

  private[this] def persistDoubleLongState(state: MeanState, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.sum)
      out.writeLong(state.count)
    }
  }

  private[this] def persistBytes(bytes: Array[Byte], identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeInt(bytes.length)
      for (index <- bytes.indices) {
        out.writeByte(bytes(index))
      }
    }
  }

  private[this] def persistFrequenciesLongState(
      state: JdbcFrequenciesAndNumRows,
      identifier: String)
    : Unit = {

    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeLong(state.frequencies.size)

      for ((key: Seq[String], value: Long) <- state.frequencies) {
        out.writeInt(key.size)
        for (string <- key) {
          out.writeInt(string.length)
          for (char <- string)
            out.writeChar(char)
        }
        out.writeLong(value)
      }
      out.writeLong(state.numRows)
    }

  }

  private[this] def persistCorrelationState(state: CorrelationState, identifier: String): Unit = {
    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
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

    LocalDiskUtils.writeToFileOnDisk(s"$locationPrefix-$identifier.bin", allowOverwrite) { out =>
      out.writeDouble(state.n)
      out.writeDouble(state.avg)
      out.writeDouble(state.m2)
    }
  }

  private[this] def loadLongState(identifier: String): Long = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") {
      _.readLong()
    }
  }

  private[this] def loadDoubleState(identifier: String): Double = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") {
      _.readDouble()
    }
  }
  private[this] def loadLongLongState(identifier: String): NumMatchesAndCount = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      NumMatchesAndCount(in.readLong(), in.readLong())
    }
  }

  private[this] def loadDoubleLongState(identifier: String): MeanState = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      MeanState(in.readDouble(), in.readLong())
    }
  }

  private[this] def loadBytes(identifier: String): Array[Byte] = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      val length = in.readInt()
      Array.fill(length) { in.readByte() }
    }
  }

  private[this] def loadFrequenciesLongState(identifier: String): JdbcFrequenciesAndNumRows = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      val numberOfBins = in.readLong()

      def readKeyValuePair(map: Map[Seq[String], Long]): Map[Seq[String], Long] = {
        if (map.size < numberOfBins) {
          val columnsAmount = in.readInt
          var columns = Seq[String]()
          for (_ <- 1 to columnsAmount) {
            val keyLength = in.readInt()
            val key: String = (for (_ <- 1 to keyLength) yield in.readChar()).mkString
            columns = columns :+ key
          }
          val value: Long = in.readLong()
          val pair = columns -> value
          readKeyValuePair(map + pair)
        } else {
          map
        }
      }

      val frequencies = readKeyValuePair(Map[Seq[String], Long]())
      val numRows = in.readLong()
      JdbcFrequenciesAndNumRows(frequencies, numRows)
    }
  }

  private[this] def loadCorrelationState(identifier: String): CorrelationState = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      CorrelationState(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(),
        in.readDouble(), in.readDouble())
    }
  }

  private[this] def loadStandardDeviationState(identifier: String): StandardDeviationState = {
    LocalDiskUtils.readFromFileOnDisk(s"$locationPrefix-$identifier.bin") { in =>
      StandardDeviationState(in.readDouble(), in.readDouble(), in.readDouble())
    }
  }
}
