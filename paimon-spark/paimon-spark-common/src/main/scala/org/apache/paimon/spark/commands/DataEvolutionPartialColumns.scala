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

package org.apache.paimon.spark.commands

import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, struct, when}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * Helpers for the conflict rewriters, which have to read and re-write the exact columns a
 * partial-column data evolution file holds. Since sub-field-level data evolution those write
 * columns may be dotted paths addressing a single leaf of a struct (e.g. `Seq("value", "nest.a")`)
 * rather than plain top-level names.
 */
private[spark] class DataEvolutionPartialColumns(table: FileStoreTable) {

  private val dataEvolutionNestedFieldEnabled =
    table.coreOptions().dataEvolutionNestedFieldEnabled()

  private lazy val fieldNames = table.rowType().getFieldNames.asScala.toSet

  /**
   * The top-level column a write path addresses. A path that names a field exactly is that field
   * even when its own name contains a dot, mirroring `RowType#projectByPaths`.
   */
  def topLevelOf(path: String): String = {
    if (!dataEvolutionNestedFieldEnabled) {
      return path
    }
    val dot = path.indexOf('.')
    if (dot < 0 || fieldNames.contains(path)) path else path.substring(0, dot)
  }

  /** Distinct top-level column names addressed by `paths`, in first-seen order. */
  def topLevelColumns(paths: Seq[String]): Seq[String] =
    paths.map(topLevelOf).distinct

  /** Whether any path addresses a sub-field rather than a whole top-level column. */
  def hasNestedPaths(paths: Seq[String]): Boolean =
    paths.exists(path => topLevelOf(path) != path)

  /**
   * The Spark type of the row a file with these write paths physically holds, i.e. the Spark view
   * of `table.rowType().projectByPaths(paths)`. Its fields are in write-path order, which is the
   * order [[DataEvolutionPaimonWriter.writePartialFields]] expects the data frame to be in.
   */
  def writeStructType(paths: Seq[String]): StructType = {
    val writeType =
      if (dataEvolutionNestedFieldEnabled) {
        table.rowType().projectByPaths(paths.asJava)
      } else {
        table.rowType().project(paths.asJava)
      }
    SparkTypeUtils.fromPaimonRowType(writeType)
  }

  /**
   * One projection per top-level column, in write-path order and aliased to the column's name. A
   * whole column is read as it is; a partially written struct is rebuilt carrying only the written
   * leaves, so re-writing the file leaves its untouched siblings alone instead of overwriting them
   * with nulls.
   */
  def projections(paths: Seq[String]): Seq[Column] = {
    val topCols = topLevelColumns(paths)
    val writeType = writeStructType(paths)
    topCols.zip(writeType.fields).map {
      case (name, field) =>
        val column = quotedColumn(name)
        field.dataType match {
          // a struct that got pruned by projectByPaths must be rebuilt to that pruned shape
          case pruned: StructType if !paths.contains(name) =>
            rebuild(column, pruned).as(name)
          case _ => column
        }
    }
  }

  /**
   * Rebuild `input` as `pruned`, keeping only the sub-fields that survive the projection. A NULL
   * struct stays NULL: rebuilding it field by field would otherwise turn it into a non-null struct
   * of NULL children.
   */
  private def rebuild(input: Column, pruned: StructType): Column = {
    val fields = pruned.fields.map {
      field =>
        val child = input.getField(field.name)
        field.dataType match {
          case nested: StructType => rebuild(child, nested).as(field.name)
          case _ => child.as(field.name)
        }
    }
    when(input.isNull, lit(null).cast(pruned)).otherwise(struct(fields: _*))
  }

  def quotedColumn(name: String): Column = col("`" + name.replace("`", "``") + "`")
}
