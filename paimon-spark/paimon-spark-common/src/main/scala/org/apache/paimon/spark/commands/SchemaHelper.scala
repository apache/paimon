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

import org.apache.paimon.options.Options
import org.apache.paimon.spark.{SparkConnectorOptions, SparkTypeUtils}
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.{Column, DataFrame, PaimonUtils, SparkSession}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.JavaConverters._

private[spark] trait SchemaHelper extends WithFileStoreTable {

  val originTable: FileStoreTable

  protected var newTable: Option[FileStoreTable] = None

  override def table: FileStoreTable = newTable.getOrElse(originTable)

  def mergeSchema(sparkSession: SparkSession, input: DataFrame, options: Options): DataFrame = {
    val dataSchema = SparkSystemColumns.filterSparkSystemColumns(input.schema)
    val newTableSchema = mergeSchema(input.schema, options)
    if (!PaimonUtils.sameType(newTableSchema, dataSchema)) {
      val resolve = sparkSession.sessionState.conf.resolver
      val cols = alignColumns(newTableSchema, dataSchema, resolve)
      input.select(cols: _*)
    } else {
      input
    }
  }

  /**
   * Recursively align columns from dataSchema to targetSchema by name. For nested struct fields,
   * reorder and fill nulls for missing sub-fields.
   */
  private def alignColumns(
      targetSchema: StructType,
      dataSchema: StructType,
      resolve: (String, String) => Boolean): Seq[Column] = {
    targetSchema.map {
      targetField =>
        dataSchema.find(f => resolve(f.name, targetField.name)) match {
          case Some(dataField) =>
            alignColumn(col(dataField.name), dataField.dataType, targetField, resolve)
          case _ =>
            lit(null).cast(targetField.dataType).as(targetField.name)
        }
    }
  }

  private def alignColumn(
      sourceCol: Column,
      sourceType: DataType,
      targetField: StructField,
      resolve: (String, String) => Boolean): Column = {
    (sourceType, targetField.dataType) match {
      case (s: StructType, t: StructType) if !PaimonUtils.sameType(s, t) =>
        val subCols = t.map {
          subTargetField =>
            s.find(f => resolve(f.name, subTargetField.name)) match {
              case Some(subDataField) =>
                alignColumn(
                  sourceCol.getField(subDataField.name),
                  subDataField.dataType,
                  subTargetField,
                  resolve)
              case _ =>
                lit(null).cast(subTargetField.dataType).as(subTargetField.name)
            }
        }
        struct(subCols: _*).as(targetField.name)
      case _ =>
        sourceCol.as(targetField.name)
    }
  }

  def mergeSchema(dataSchema: StructType, options: Options): StructType = {
    val mergeSchemaEnabled =
      options.get(SparkConnectorOptions.MERGE_SCHEMA) || OptionUtils.writeMergeSchemaEnabled()
    if (!mergeSchemaEnabled) {
      return dataSchema
    }

    val filteredDataSchema = SparkSystemColumns.filterSparkSystemColumns(dataSchema)
    val allowExplicitCast = options.get(SparkConnectorOptions.EXPLICIT_CAST) || OptionUtils
      .writeMergeSchemaExplicitCastEnabled()
    mergeAndCommitSchema(filteredDataSchema, allowExplicitCast)

    val writeSchema = SparkTypeUtils.fromPaimonRowType(table.schema().logicalRowType())

    if (!PaimonUtils.sameType(writeSchema, filteredDataSchema)) {
      writeSchema
    } else {
      filteredDataSchema
    }
  }

  private def mergeAndCommitSchema(dataSchema: StructType, allowExplicitCast: Boolean): Unit = {
    val dataRowType = SparkTypeUtils.toPaimonType(dataSchema).asInstanceOf[RowType]
    if (table.store().mergeSchema(dataRowType, allowExplicitCast)) {
      newTable = Some(table.copyWithLatestSchema())
    }
  }

  def updateTableWithOptions(options: Map[String, String]): Unit = {
    newTable = Some(table.copy(options.asJava))
  }
}
