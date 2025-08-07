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

import org.apache.spark.sql.{DataFrame, PaimonUtils, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

private[spark] trait SchemaHelper extends WithFileStoreTable {

  val originTable: FileStoreTable

  protected var newTable: Option[FileStoreTable] = None

  override def table: FileStoreTable = newTable.getOrElse(originTable)

  def mergeSchema(sparkSession: SparkSession, input: DataFrame, options: Options): DataFrame = {
    val mergeSchemaEnabled =
      options.get(SparkConnectorOptions.MERGE_SCHEMA) || OptionUtils.writeMergeSchemaEnabled()
    if (!mergeSchemaEnabled) {
      return input
    }

    val dataSchema = SparkSystemColumns.filterSparkSystemColumns(input.schema)
    val allowExplicitCast = options.get(SparkConnectorOptions.EXPLICIT_CAST) || OptionUtils
      .writeMergeSchemaExplicitCastEnabled()
    mergeAndCommitSchema(dataSchema, allowExplicitCast)

    // For case that some columns is absent in data, we still allow to write once write.merge-schema is true.
    val newTableSchema = SparkTypeUtils.fromPaimonRowType(table.schema().logicalRowType())
    if (!PaimonUtils.sameType(newTableSchema, dataSchema)) {
      val resolve = sparkSession.sessionState.conf.resolver
      val cols = newTableSchema.map {
        field =>
          dataSchema.find(f => resolve(f.name, field.name)) match {
            case Some(f) => col(f.name)
            case _ => lit(null).as(field.name)
          }
      }
      input.select(cols: _*)
    } else {
      input
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
