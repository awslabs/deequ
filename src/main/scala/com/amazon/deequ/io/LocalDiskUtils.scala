package com.amazon.deequ.io

import java.io.{DataInputStream, DataOutputStream, FileInputStream, FileOutputStream}


private[deequ] object LocalDiskUtils {

  /* Helper function to read from a binary file on S3 */
  def readFromFileOnDisk[T](path: String)
                           (readFunc: DataInputStream => T): T = {

    val input = new DataInputStream(new FileInputStream(path))

    try {
      readFunc(input)
    } finally {
      if (input != null) {
        input.close()
      }
    }
  }

  /* Helper function to write to a binary file on S3 */
  def writeToFileOnDisk(path: String, overwrite: Boolean = false)
                       (writeFunc: DataOutputStream => Unit): Unit = {

    val output = new DataOutputStream(new FileOutputStream(path))

    try {
      writeFunc(output)
    } finally {
      if (output != null) {
        output.close()
      }
    }
  }

}
