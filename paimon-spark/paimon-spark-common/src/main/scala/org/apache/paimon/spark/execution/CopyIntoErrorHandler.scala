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

package org.apache.paimon.spark.execution

import org.apache.paimon.spark.catalyst.plans.logical.FileFormatType
import org.apache.paimon.spark.copyinto.{CopyIntoResultBuilder, CopyLoadHistoryManager, CopyLoadRecord}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.DataField

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Handles error detection and result building for COPY INTO operations. Supports both row-level
 * (CONTINUE) and file-level (SKIP_FILE) error handling.
 */
private[execution] class CopyIntoErrorHandler(
    spark: org.apache.spark.sql.SparkSession,
    castValidator: CopyIntoCastValidator,
    dataFrameBuilder: CopyIntoDataFrameBuilder) {

  /** Detect cast errors in Parquet files. Returns error statistics and good rows DataFrame. */
  def detectParquetErrors(
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField])(rawDfWithFile: DataFrame, fileCol: String): ErrorDetectionResult = {

    val totalRowsPerFile = CopyIntoUtils.countPerFile(rawDfWithFile, fileCol)

    val castResult =
      filterParquetCastErrors(rawDfWithFile, targetColumns, writableColumns, fields, fileCol)

    ErrorDetectionResult(
      totalRowsPerFile = totalRowsPerFile,
      parseErrors = Map.empty, // Parquet has no parse errors
      castErrors = castResult.errorsPerFile,
      firstParseError = Map.empty,
      firstCastError = castResult.firstErrorPerFile,
      goodRowsDf = castResult.df
    )
  }

  /**
   * Detect parse and cast errors in text files (CSV/JSON). Returns error statistics and good rows
   * DataFrame (already processed and cast).
   */
  def detectTextErrors(
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      stringSchema: StructType,
      corruptCol: String)(rawDfWithFile: DataFrame, fileCol: String): ErrorDetectionResult = {

    val totalRowsPerFile = CopyIntoUtils.countPerFile(rawDfWithFile, fileCol)

    // Separate corrupt rows (parse errors) from valid rows
    val corruptDf = rawDfWithFile.filter(col(corruptCol).isNotNull)
    val validDf = rawDfWithFile.filter(col(corruptCol).isNull).drop(corruptCol)

    val parseErrors = CopyIntoUtils.countPerFile(corruptDf, fileCol)
    val firstParseError = if (parseErrors.nonEmpty) {
      val samplesPerFile = corruptDf
        .select(col(fileCol), col(corruptCol))
        .dropDuplicates(fileCol)
        .collect()
      samplesPerFile.map {
        row =>
          CopyIntoUtils.extractBaseName(
            row.getString(0)) -> s"Malformed record: ${row.getString(1)}"
      }.toMap
    } else Map.empty[String, String]

    // Process valid rows: apply null transforms, build final DataFrame, detect cast errors
    val processedDf = dataFrameBuilder.applyNullTransforms(validDf, stringSchema.fieldNames)
    val finalDfWithFile = dataFrameBuilder.buildFinalDataFrame(
      processedDf,
      targetColumns,
      writableColumns,
      fields,
      extraCols = Seq(fileCol))

    val castResult = castAndFilterErrors(finalDfWithFile, writableColumns, fields, fileCol)

    ErrorDetectionResult(
      totalRowsPerFile = totalRowsPerFile,
      parseErrors = parseErrors,
      castErrors = castResult.errorsPerFile,
      firstParseError = firstParseError,
      firstCastError = castResult.firstErrorPerFile,
      goodRowsDf = castResult.df
    )
  }

  /**
   * Build results for error-tolerant modes (CONTINUE/SKIP_FILE). Handles both row-level and
   * file-level error reporting.
   */
  def buildErrorTolerantResults(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      errorResult: ErrorDetectionResult,
      filesWithErrors: Set[String],
      errorGranularity: ErrorGranularity): Seq[InternalRow] = {

    errorGranularity match {
      case ErrorGranularity.RowLevel =>
        // CONTINUE mode: use existing buildContinueResults
        CopyIntoResultBuilder.buildContinueResults(
          paimonTable,
          filesToLoad,
          skippedFiles,
          errorResult.totalRowsPerFile,
          errorResult.parseErrors,
          errorResult.castErrors,
          errorResult.firstParseError,
          errorResult.firstCastError
        )

      case ErrorGranularity.FileLevel =>
        // SKIP_FILE mode: files are either fully loaded or fully failed
        val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
        val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)
        val snapshotId = paimonTable.snapshotManager().latestSnapshotId()
        val loadedAt = System.currentTimeMillis()

        val results = filesToLoad.map {
          fileStatus =>
            val baseName = fileStatus.getPath.getName
            val fullPath = fileStatus.getPath.toString

            if (filesWithErrors.contains(baseName)) {
              // File had errors, mark as LOAD_FAILED
              val parseErrorCount = errorResult.parseErrors.getOrElse(baseName, 0L)
              val castErrorCount = errorResult.castErrors.getOrElse(baseName, 0L)
              val totalErrors = parseErrorCount + castErrorCount
              val firstError = errorResult.firstParseError
                .get(baseName)
                .orElse(errorResult.firstCastError.get(baseName))

              CopyIntoResultBuilder.buildResultRow(
                baseName,
                "LOAD_FAILED",
                0L,
                errorResult.totalRowsPerFile.getOrElse(baseName, 0L),
                totalErrors,
                firstError.orNull)
            } else {
              // File had no errors, mark as LOADED
              val rowCount = errorResult.totalRowsPerFile.getOrElse(baseName, 0L)
              historyManager.recordLoaded(
                CopyLoadRecord(
                  filePath = fullPath,
                  fileSize = fileStatus.getLen,
                  lastModified = fileStatus.getModificationTime,
                  loadedAt = loadedAt,
                  snapshotId = snapshotId,
                  rowsLoaded = rowCount
                ))

              CopyIntoResultBuilder.buildResultRow(baseName, "LOADED", rowCount, rowCount, 0L, null)
            }
        }.toSeq

        results ++ CopyIntoResultBuilder.buildSkippedResults(skippedFiles)
    }
  }

  /**
   * Filter out rows with cast errors and collect per-file error stats. Shared by both Parquet and
   * text CONTINUE paths.
   */
  private def filterCastErrors(setup: CastValidationSetup, fileCol: String): CastFilterResult = {
    setup.badCastFilter match {
      case Some(filter) =>
        val badRowsDf = setup.validationDf.filter(filter)

        // Count errors and sample first error per file
        val sampleRows = badRowsDf.dropDuplicates(fileCol).collect()
        val errorsPerFile = CopyIntoUtils.countPerFile(badRowsDf, fileCol)

        val firstErrorPerFile = if (sampleRows.nonEmpty) {
          sampleRows.map {
            sampleRow =>
              val fileName =
                CopyIntoUtils.extractBaseName(sampleRow.getString(sampleRow.fieldIndex(fileCol)))
              val example = setup.castColMapping.find {
                case (src, dst) =>
                  val srcIdx = setup.validationDf.schema.fieldIndex(src)
                  val dstIdx = setup.validationDf.schema.fieldIndex(dst)
                  !sampleRow.isNullAt(srcIdx) && sampleRow.isNullAt(dstIdx)
              }
              fileName -> s"Cast failure in column '${example.map(_._1).getOrElse("unknown")}'. Source data contains values that cannot be converted to the target type."
          }.toMap
        } else Map.empty[String, String]

        // Keep good rows and drop validation temp columns
        var goodDf = setup.validationDf.filter(!filter)
        setup.castColMapping.values.foreach(c => goodDf = goodDf.drop(c))

        CastFilterResult(goodDf, errorsPerFile, firstErrorPerFile)
      case None =>
        CastFilterResult(setup.validationDf, Map.empty, Map.empty)
    }
  }

  private def filterParquetCastErrors(
      rawDfWithFile: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      fileCol: String): CastFilterResult = {
    val setup =
      castValidator.buildParquetCastValidation(
        rawDfWithFile,
        targetColumns,
        writableColumns,
        fields,
        excludeCols = Set(fileCol))
    filterCastErrors(setup, fileCol)
  }

  private def castAndFilterErrors(
      dfWithFile: DataFrame,
      writableColumns: Seq[String],
      fields: Seq[DataField],
      fileCol: String): CastFilterResult = {
    val setup = castValidator.buildTextCastValidation(dfWithFile, writableColumns, fields)
    val result = filterCastErrors(setup, fileCol)
    // Apply final cast to target types for good rows
    CastFilterResult(
      castValidator.castColumns(result.df, writableColumns, fields),
      result.errorsPerFile,
      result.firstErrorPerFile)
  }
}

private[execution] case class CastFilterResult(
    df: DataFrame,
    errorsPerFile: Map[String, Long],
    firstErrorPerFile: Map[String, String])

/** Error granularity for error-tolerant modes (CONTINUE/SKIP_FILE). */
sealed private[execution] trait ErrorGranularity
private[execution] object ErrorGranularity {
  case object RowLevel extends ErrorGranularity
  case object FileLevel extends ErrorGranularity
}

/** Unified error detection result for both Parquet and text formats. */
private[execution] case class ErrorDetectionResult(
    totalRowsPerFile: Map[String, Long],
    parseErrors: Map[String, Long],
    castErrors: Map[String, Long],
    firstParseError: Map[String, String],
    firstCastError: Map[String, String],
    goodRowsDf: DataFrame)
