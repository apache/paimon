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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.plans.logical.CopyFileFormat
import org.apache.paimon.spark.copyinto.{CopyLoadHistoryManager, CopyLoadRecord}
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.DataField

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.functions.{col, input_file_name, lit, when}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class CopyIntoTableExec(
    spark: SparkSession,
    catalog: TableCatalog,
    ident: Identifier,
    sourcePath: String,
    columns: Option[Seq[String]],
    fileFormat: CopyFileFormat,
    pattern: Option[String],
    force: Boolean,
    out: Seq[Attribute])
  extends PaimonLeafV2CommandExec {

  override def output: Seq[Attribute] = out

  override protected def run(): Seq[InternalRow] = {
    fileFormat.validateForImport()

    val table = catalog.loadTable(ident)
    assert(table.isInstanceOf[SparkTable])
    val paimonTable = table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]
    val tableSchema = paimonTable.schema()
    val writableColumns = tableSchema.fieldNames().asScala.toSeq
    val fields = tableSchema.fields().asScala.toSeq
    val targetColumns = resolveTargetColumns(writableColumns)

    validateNonNullableDefaults(writableColumns, targetColumns, fields)

    val (filesToLoad, skippedFiles) = listAndFilterFiles(paimonTable)

    if (filesToLoad.isEmpty) {
      return buildSkippedResults(skippedFiles)
    }

    val filePaths = filesToLoad.map(_.getPath.toString)
    val stringSchema = StructType(
      (0 until targetColumns.size).map(i => StructField(s"_c$i", StringType, nullable = true)))
    val readerOptions = fileFormat.toSparkReaderOptions

    val csvDf = readAndProcessCsv(filePaths, stringSchema, readerOptions)
    val finalDf =
      buildFinalDataFrame(csvDf, targetColumns, writableColumns, fields)
    val castedDf = castAndValidate(finalDf, writableColumns, fields)

    val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
    castedDf.write.format("paimon").mode("append").insertInto(tableName)

    recordHistoryAndBuildResults(
      paimonTable,
      filesToLoad,
      skippedFiles,
      filePaths,
      stringSchema,
      readerOptions)
  }

  private def resolveTargetColumns(writableColumns: Seq[String]): Seq[String] = {
    columns match {
      case Some(cols) =>
        val resolver = spark.sessionState.conf.resolver
        cols.indices.foreach {
          i =>
            cols.indices.filter(_ > i).foreach {
              j =>
                if (resolver(cols(i), cols(j))) {
                  throw new IllegalArgumentException(
                    s"Duplicate columns in column list: ${cols(i)}")
                }
            }
        }
        cols.map {
          c =>
            writableColumns.find(w => resolver(w, c)).getOrElse {
              throw new IllegalArgumentException(
                s"Column '$c' does not exist in target table. Available columns: ${writableColumns.mkString(", ")}")
            }
        }
      case None => writableColumns
    }
  }

  private def validateNonNullableDefaults(
      writableColumns: Seq[String],
      targetColumns: Seq[String],
      fields: Seq[DataField]): Unit = {
    if (columns.isEmpty) return
    val unmapped = writableColumns.filterNot(targetColumns.contains)
    unmapped.foreach {
      colName =>
        val field = fields.find(_.name() == colName).get
        if (!field.`type`().isNullable && field.defaultValue() == null) {
          throw new IllegalArgumentException(
            s"Non-nullable column '$colName' is not in the column list and has no default value")
        }
    }
  }

  private def listAndFilterFiles(
      paimonTable: FileStoreTable): (Array[FileStatus], Array[FileStatus]) = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fsPath = new Path(sourcePath)
    val fs = fsPath.getFileSystem(hadoopConf)
    val allFiles = fs.listStatus(fsPath).filter(_.isFile)

    val patternFiltered = pattern match {
      case Some(p) =>
        val regex = p.r
        allFiles.filter(f => regex.findFirstIn(f.getPath.getName).isDefined)
      case None => allFiles
    }

    if (patternFiltered.isEmpty) {
      return (Array.empty, Array.empty)
    }

    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)

    if (!force) {
      val (skip, load) = patternFiltered.partition {
        f => historyManager.isLoaded(f.getPath.toString, f.getLen, f.getModificationTime)
      }
      (load, skip)
    } else {
      (patternFiltered, Array.empty[FileStatus])
    }
  }

  private def readAndProcessCsv(
      filePaths: Array[String],
      stringSchema: StructType,
      readerOptions: Map[String, String]): DataFrame = {
    var df = spark.read
      .options(readerOptions)
      .schema(stringSchema)
      .csv(filePaths: _*)

    val nullIfVals = fileFormat.nullIfValues
    if (nullIfVals.nonEmpty) {
      df.columns.foreach {
        colName =>
          df = df.withColumn(
            colName,
            when(col(colName).isin(nullIfVals: _*), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    if (fileFormat.emptyFieldAsNull) {
      df.columns.foreach {
        colName =>
          df = df.withColumn(
            colName,
            when(col(colName) === lit(""), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    df
  }

  private def buildFinalDataFrame(
      csvDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
    val renamedDf = targetColumns.zipWithIndex.foldLeft(csvDf) {
      case (df, (targetCol, idx)) => df.withColumnRenamed(s"_c$idx", targetCol)
    }

    if (columns.isDefined) {
      val selectExprs: Seq[Column] = writableColumns.map {
        colName =>
          if (targetColumns.contains(colName)) {
            col(colName)
          } else {
            val field = fields.find(_.name() == colName).get
            val defaultVal = field.defaultValue()
            if (defaultVal != null) {
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              try {
                val parsed = spark.sessionState.sqlParser.parseExpression(defaultVal)
                SparkShimLoader.shim.classicApi.column(parsed).cast(sparkType).as(colName)
              } catch {
                case _: Exception => lit(null).cast(sparkType).as(colName)
              }
            } else {
              lit(null).as(colName)
            }
          }
      }
      renamedDf.select(selectExprs: _*)
    } else {
      renamedDf
    }
  }

  private def castAndValidate(
      finalDf: DataFrame,
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
    val nonStringCastCols = ArrayBuffer[String]()
    var castedDf = finalDf
    writableColumns.zip(fields).foreach {
      case (colName, field) =>
        val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
        castedDf = castedDf.withColumn(colName, col(colName).cast(sparkType))
        if (sparkType != StringType) {
          nonStringCastCols += colName
        }
    }

    if (nonStringCastCols.nonEmpty) {
      val castSuffix = "__cv"
      val validationDf = nonStringCastCols.zipWithIndex.foldLeft(finalDf) {
        case (df, (colName, idx)) =>
          val field = fields.find(_.name() == colName).get
          val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
          df.withColumn(castSuffix + idx, col(colName).cast(sparkType))
      }
      val badCastFilter = nonStringCastCols.zipWithIndex
        .map { case (cn, idx) => col(cn).isNotNull && col(castSuffix + idx).isNull }
        .reduce(_ || _)
      val badRows = validationDf.filter(badCastFilter).limit(1).collect()
      if (badRows.nonEmpty) {
        val example = nonStringCastCols.zipWithIndex.find {
          case (cn, idx) =>
            val row = badRows(0)
            val srcIdx = validationDf.schema.fieldIndex(cn)
            val dstIdx = validationDf.schema.fieldIndex(castSuffix + idx)
            !row.isNullAt(srcIdx) && row.isNullAt(dstIdx)
        }
        throw new IllegalArgumentException(
          s"ON_ERROR = ABORT_STATEMENT: Cast failure in column '${example.map(_._1).getOrElse("unknown")}'. Source data contains values that cannot be converted to the target type.")
      }
    }

    castedDf
  }

  private def recordHistoryAndBuildResults(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      filePaths: Array[String],
      stringSchema: StructType,
      readerOptions: Map[String, String]): Seq[InternalRow] = {
    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)
    val snapshotId = paimonTable.snapshotManager().latestSnapshotId()
    val loadedAt = System.currentTimeMillis()

    val rowCounts = spark.read
      .options(readerOptions)
      .schema(stringSchema)
      .csv(filePaths: _*)
      .groupBy(input_file_name().as("file"))
      .count()
      .collect()

    val fileCountMap = rowCounts.map {
      row =>
        val fullPath = row.getString(0)
        val baseName = fullPath.substring(fullPath.lastIndexOf('/') + 1)
        baseName -> row.getLong(1)
    }.toMap

    val loadedResults = filesToLoad.map {
      fileStatus =>
        val baseName = fileStatus.getPath.getName
        val rowCount = fileCountMap.getOrElse(baseName, 0L)

        historyManager.recordLoaded(
          CopyLoadRecord(
            filePath = fileStatus.getPath.toString,
            fileSize = fileStatus.getLen,
            lastModified = fileStatus.getModificationTime,
            loadedAt = loadedAt,
            snapshotId = snapshotId,
            rowsLoaded = rowCount
          ))

        InternalRow(
          UTF8String.fromString(baseName),
          UTF8String.fromString("LOADED"),
          rowCount,
          rowCount)
    }.toSeq

    val skippedResults = buildSkippedResults(skippedFiles)
    loadedResults ++ skippedResults
  }

  private def buildSkippedResults(files: Array[FileStatus]): Seq[InternalRow] = {
    files.map {
      f =>
        InternalRow(
          UTF8String.fromString(f.getPath.getName),
          UTF8String.fromString("SKIPPED"),
          0L,
          0L)
    }.toSeq
  }
}
