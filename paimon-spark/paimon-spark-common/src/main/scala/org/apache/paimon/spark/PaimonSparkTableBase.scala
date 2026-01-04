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
import org.apache.paimon.CoreOptions.BucketFunctionType
import org.apache.paimon.options.Options
import org.apache.paimon.spark.catalog.functions.BucketFunction
import org.apache.paimon.spark.scan.PaimonSplitScanBuilder
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.spark.write.{PaimonV2WriteBuilder, PaimonWriteBuilder}
import org.apache.paimon.table.{Table, _}
import org.apache.paimon.table.BucketMode.{BUCKET_UNAWARE, HASH_FIXED, POSTPONE_MODE}

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Collections, EnumSet => JEnumSet, HashMap => JHashMap, Map => JMap, Set => JSet}

import scala.collection.mutable.ArrayBuffer

abstract class PaimonSparkTableBase(val table: Table)
  extends BaseTable
  with SupportsRead
  with SupportsWrite
  with TruncatableTable
  with SupportsMetadataColumns {

  lazy val coreOptions = new CoreOptions(table.options())

  lazy val useV2Write: Boolean = {
    val v2WriteConfigured = OptionUtils.useV2Write()
    v2WriteConfigured && supportsV2Write
  }

  private def supportsV2Write: Boolean = {
    coreOptions.bucketFunctionType() == BucketFunctionType.DEFAULT && {
      table match {
        case storeTable: FileStoreTable =>
          storeTable.bucketMode() match {
            case HASH_FIXED => BucketFunction.supportsTable(storeTable)
            case BUCKET_UNAWARE => true
            case POSTPONE_MODE if !coreOptions.postponeBatchWriteFixedBucket() => true
            case _ => false
          }

        case _ => false
      }
    } && coreOptions.clusteringColumns().isEmpty
  }

  def getTable: Table = table

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
        if (properties.containsKey(CoreOptions.PATH.key())) {
          properties.put(TableCatalog.PROP_LOCATION, properties.get(CoreOptions.PATH.key()))
        }
        properties
      case _ => Collections.emptyMap()
    }
  }

  override def capabilities: JSet[TableCapability] = {
    val capabilities = JEnumSet.of(
      TableCapability.BATCH_READ,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.MICRO_BATCH_READ
    )

    if (useV2Write) {
      capabilities.add(TableCapability.ACCEPT_ANY_SCHEMA)
      capabilities.add(TableCapability.BATCH_WRITE)
      capabilities.add(TableCapability.OVERWRITE_DYNAMIC)
    } else {
      capabilities.add(TableCapability.ACCEPT_ANY_SCHEMA)
      capabilities.add(TableCapability.V1_BATCH_WRITE)
    }

    capabilities
  }

  override def metadataColumns: Array[MetadataColumn] = {
    val partitionType = SparkTypeUtils.toSparkPartitionType(table)

    val _metadataColumns = ArrayBuffer[MetadataColumn]()

    if (coreOptions.rowTrackingEnabled()) {
      _metadataColumns.append(PaimonMetadataColumn.ROW_ID)
      _metadataColumns.append(PaimonMetadataColumn.SEQUENCE_NUMBER)
    }

    _metadataColumns.appendAll(
      Seq(
        PaimonMetadataColumn.FILE_PATH,
        PaimonMetadataColumn.ROW_INDEX,
        PaimonMetadataColumn.PARTITION(partitionType),
        PaimonMetadataColumn.BUCKET
      ))

    _metadataColumns.toArray
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    table match {
      case t: KnownSplitsTable =>
        new PaimonSplitScanBuilder(t)
      case _: InnerTable =>
        new PaimonScanBuilder(table.copy(options.asCaseSensitiveMap).asInstanceOf[InnerTable])
      case _ =>
        throw new RuntimeException("Only InnerTable can be scanned.")
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val options = Options.fromMap(info.options)
        if (useV2Write) {
          new PaimonV2WriteBuilder(fileStoreTable, info.schema(), options)
        } else {
          new PaimonWriteBuilder(fileStoreTable, options)
        }
      case _ =>
        throw new RuntimeException("Only FileStoreTable can be written.")
    }
  }

  def truncateTable: Boolean = {
    val commit = table.newBatchWriteBuilder().newCommit()
    commit.truncateTable()
    true
  }
}
