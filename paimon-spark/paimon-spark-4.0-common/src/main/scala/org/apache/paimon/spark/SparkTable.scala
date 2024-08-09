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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.options.Options
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{DataTable, FileStoreTable, Table}

import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsMetadataColumns, SupportsRead, SupportsWrite, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Collections, EnumSet => JEnumSet, HashMap => JHashMap, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._

/** A spark [[org.apache.spark.sql.connector.catalog.Table]] for paimon. */
case class SparkTable(table: Table)
  extends org.apache.spark.sql.connector.catalog.Table
  with SupportsRead
  with SupportsWrite
  with SupportsMetadataColumns
  with PaimonPartitionManagement {

  def getTable: Table = table

  override def name: String = table.fullName

  override lazy val schema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType)

  override def partitioning: Array[Transform] = {
    table.partitionKeys().asScala.map(p => Expressions.identity(p)).toArray
  }

  override def properties: JMap[String, String] = {
    table match {
      case dataTable: DataTable =>
        val properties = new JHashMap[String, String](dataTable.coreOptions.toMap)
        if (!table.primaryKeys.isEmpty) {
          properties.put(CoreOptions.PRIMARY_KEY.key, String.join(",", table.primaryKeys))
        }
        properties.put(TableCatalog.PROP_PROVIDER, SparkSource.NAME)
        if (table.comment.isPresent) {
          properties.put(TableCatalog.PROP_COMMENT, table.comment.get)
        }
        properties
      case _ => Collections.emptyMap()
    }
  }

  override def capabilities: JSet[TableCapability] = {
    JEnumSet.of(
      TableCapability.ACCEPT_ANY_SCHEMA,
      TableCapability.BATCH_READ,
      TableCapability.V1_BATCH_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.OVERWRITE_DYNAMIC,
      TableCapability.MICRO_BATCH_READ
    )
  }

  override def metadataColumns: Array[MetadataColumn] = {
    Array[MetadataColumn](PaimonMetadataColumn.FILE_PATH, PaimonMetadataColumn.ROW_INDEX)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new PaimonScanBuilder(table.copy(options.asCaseSensitiveMap))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    table match {
      case fileStoreTable: FileStoreTable =>
        new SparkWriteBuilder(fileStoreTable, Options.fromMap(info.options))
      case _ =>
        throw new RuntimeException("Only FileStoreTable can be written.")
    }
  }
}
