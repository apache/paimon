/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.copyinto

import org.apache.paimon.table.FileStoreTable

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

/** Builds per-file result rows for COPY INTO commands. */
object CopyIntoResultBuilder {

  /** Build a single result row with the standard 6-column schema. */
  def buildResultRow(
      fileName: String,
      status: String,
      rowsLoaded: Long,
      rowsParsed: Long,
      errorsSeen: Long,
      firstError: String): InternalRow = {
    InternalRow(
      UTF8String.fromString(fileName),
      UTF8String.fromString(status),
      rowsLoaded,
      rowsParsed,
      errorsSeen,
      if (firstError != null) UTF8String.fromString(firstError) else null
    )
  }

  /**
   * Build per-file result rows for ON_ERROR=CONTINUE mode. Merges parse errors and cast errors per
   * file, determines status (LOADED / PARTIALLY_LOADED / LOAD_FAILED), and records load history for
   * files that loaded at least one row or had no errors (including empty files).
   */
  def buildContinueResults(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      totalRowsPerFile: Map[String, Long],
      parseErrors: Map[String, Long],
      castErrors: Map[String, Long],
      firstParseErrorPerFile: Map[String, String],
      firstCastErrorPerFile: Map[String, String]): Seq[InternalRow] = {
    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)
    val snapshotId = paimonTable.snapshotManager().latestSnapshotId()
    val loadedAt = System.currentTimeMillis()

    val loadedResults = filesToLoad.map {
      fileStatus =>
        val baseName = fileStatus.getPath.getName
        val fullPath = fileStatus.getPath.toString
        val parsedCount = totalRowsPerFile.getOrElse(baseName, 0L)
        val fileParseErrors = parseErrors.getOrElse(baseName, 0L)
        val fileCastErrors = castErrors.getOrElse(baseName, 0L)
        val totalFileErrors = fileParseErrors + fileCastErrors
        val rowsLoaded = Math.max(0, parsedCount - totalFileErrors)

        if (rowsLoaded > 0 || totalFileErrors == 0) {
          historyManager.recordLoaded(
            CopyLoadRecord(
              filePath = fullPath,
              fileSize = fileStatus.getLen,
              lastModified = fileStatus.getModificationTime,
              loadedAt = loadedAt,
              snapshotId = snapshotId,
              rowsLoaded = rowsLoaded
            ))
        }

        val status =
          if (rowsLoaded == 0 && totalFileErrors > 0) "LOAD_FAILED"
          else if (totalFileErrors > 0) "PARTIALLY_LOADED"
          else "LOADED"
        val fileFirstError =
          firstParseErrorPerFile.get(baseName).orElse(firstCastErrorPerFile.get(baseName))
        buildResultRow(
          baseName,
          status,
          rowsLoaded,
          parsedCount,
          totalFileErrors,
          fileFirstError.orNull)
    }.toSeq

    loadedResults ++ buildSkippedResults(skippedFiles)
  }

  /**
   * Record load history and build results using pre-computed per-file row counts (ABORT mode).
   * Avoids re-reading source files just to count rows.
   */
  def recordHistoryAndBuildResultsDirect(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      rowCountsPerFile: Map[String, Long]): Seq[InternalRow] = {
    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)
    val snapshotId = paimonTable.snapshotManager().latestSnapshotId()
    val loadedAt = System.currentTimeMillis()

    val loadedResults = filesToLoad.map {
      fileStatus =>
        val baseName = fileStatus.getPath.getName
        val rowCount = rowCountsPerFile.getOrElse(baseName, 0L)

        historyManager.recordLoaded(
          CopyLoadRecord(
            filePath = fileStatus.getPath.toString,
            fileSize = fileStatus.getLen,
            lastModified = fileStatus.getModificationTime,
            loadedAt = loadedAt,
            snapshotId = snapshotId,
            rowsLoaded = rowCount
          ))

        buildResultRow(baseName, "LOADED", rowCount, rowCount, 0L, null)
    }.toSeq

    loadedResults ++ buildSkippedResults(skippedFiles)
  }

  def buildSkippedResults(files: Array[FileStatus]): Seq[InternalRow] = {
    files.map(f => buildResultRow(f.getPath.getName, "SKIPPED", 0L, 0L, 0L, null)).toSeq
  }
}
