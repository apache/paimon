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

import org.apache.paimon.spark.copyinto.CopyLoadHistoryManager
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.DataField

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession

/**
 * Utility methods for COPY INTO operations. Provides helper functions for column resolution, file
 * filtering, and validation.
 */
private[execution] object CopyIntoHelper {

  /**
   * Resolve target columns from user-specified column list or use all writable columns. Validates
   * that specified columns exist in the table and are not duplicated.
   */
  def resolveTargetColumns(
      spark: SparkSession,
      columns: Option[Seq[String]],
      writableColumns: Seq[String]): Seq[String] = {
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

  /**
   * Validate that non-nullable columns without default values are included in the target column
   * list. Throws exception if validation fails.
   */
  def validateNonNullableDefaults(
      columns: Option[Seq[String]],
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

  /**
   * List files from source path and filter based on pattern and load history. Returns (files to
   * load, files to skip).
   */
  def listAndFilterFiles(
      spark: SparkSession,
      paimonTable: FileStoreTable,
      sourcePath: String,
      pattern: Option[String],
      force: Boolean): (Array[FileStatus], Array[FileStatus]) = {
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

  /**
   * Generate a safe temporary column name that doesn't conflict with existing columns.
   * Case-insensitive conflict detection.
   */
  def safeTempCol(spark: SparkSession, baseName: String, existingColumns: Set[String]): String = {
    val resolver = spark.sessionState.conf.resolver
    var candidate = baseName
    while (existingColumns.exists(c => resolver(c, candidate))) {
      candidate = "_" + candidate
    }
    candidate
  }
}
