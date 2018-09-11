package com.amazon.deequ.utils

import java.nio.file.Files
import java.util.UUID

object TempFileUtils {
  def tempDir(prefix: String = UUID.randomUUID().toString): String = {
    val tempDir = Files.createTempDirectory(prefix).toFile
    tempDir.deleteOnExit()
    tempDir.getAbsolutePath
  }
}
