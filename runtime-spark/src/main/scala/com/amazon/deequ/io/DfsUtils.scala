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

package com.amazon.deequ.io

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession

private[deequ] object DfsUtils {

  /* Helper function to read from a binary file on S3 */
  def readFromFileOnDfs[T](session: SparkSession, path: String)
    (readFunc: FSDataInputStream => T): T = {

    val (fs, qualifiedPath) = asQualifiedPath(session, path)
    val input = fs.open(qualifiedPath)

    try {
      readFunc(input)
    } finally {
      if (input != null) {
        input.close()
      }
    }
  }

  /* Helper function to write to a binary file on S3 */
  def writeToFileOnDfs(session: SparkSession, path: String, overwrite: Boolean = false)
    (writeFunc: FSDataOutputStream => Unit): Unit = {

    val (fs, qualifiedPath) = asQualifiedPath(session, path)
    val output = fs.create(qualifiedPath, overwrite)

    try {
      writeFunc(output)
    } finally {
      if (output != null) {
        output.close()
      }
    }
  }

  /* Helper function to write to a binary file on S3 */
  def writeToTextFileOnDfs(session: SparkSession, path: String, overwrite: Boolean = false)
    (writeFunc: BufferedWriter => Unit): Unit = {

    val (fs, qualifiedPath) = asQualifiedPath(session, path)
    val output = fs.create(qualifiedPath, overwrite)

    try {
      val writer = new BufferedWriter(new OutputStreamWriter(output))
      writeFunc(writer)
      writer.close()
    } finally {
      if (output != null) {
        output.close()
      }
    }
  }

  /* Make sure we write to the correct filesystem, as EMR clusters also have an internal HDFS */
  def asQualifiedPath(session: SparkSession, path: String): (FileSystem, Path) = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(session.sparkContext.hadoopConfiguration)
    val qualifiedPath = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    (fs, qualifiedPath)
  }

}
