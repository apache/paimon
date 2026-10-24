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

package org.apache.spark.sql.execution

import org.apache.paimon.partition.PartitionStatistics
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalog.SparkBaseCatalog
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.spark.util.PartitionStatisticsDisplay
import org.apache.paimon.spark.utils.CatalogUtils.{checkNamespace, toIdentifier}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogStorageFormat, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class PaimonDescribeTableExec(
    output: Seq[Attribute],
    catalog: SparkBaseCatalog,
    identifier: Identifier,
    table: SparkTable,
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean)
  extends PaimonLeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val rows =
      ArrayBuffer.empty ++= SparkShimLoader.shim
        .createDescribeTableExec(output, catalog.name(), identifier, table, isExtended)
        .executeCollect()

    if (partitionSpec.nonEmpty) {
      describeDetailedPartitionInfo(rows)
    }

    if (isExtended) {
      addNotNullMetadata(table.schema, rows)
    }

    rows.toSeq
  }

  private def describeDetailedPartitionInfo(rows: ArrayBuffer[InternalRow]): Unit = {
    if (
      table.partitionSchema.fieldNames.length != partitionSpec.size
      || partitionSpec.keys.exists(!table.partitionSchema.fieldNames.contains(_))
    ) {
      throw new UnsupportedOperationException(
        "DESCRIBE partition must specify all the correct partition columns")
    }

    if (isExtended) {
      describeFormattedDetailedPartitionInfo(rows)
    }
  }

  private def describeFormattedDetailedPartitionInfo(rows: ArrayBuffer[InternalRow]): Unit = {
    checkNamespace(identifier.namespace(), catalog.name())
    rows += emptyRow()
    rows += toCatalystRow("# Detailed Partition Information", "", "")
    rows += toCatalystRow("Database", identifier.namespace().head, "")
    rows += toCatalystRow("Table", identifier.name(), "")
    val partition = catalog
      .paimonCatalog()
      .listPartitions(toIdentifier(identifier, catalog.name()))
      .asScala
      .filter(_.spec().asScala == partitionSpec)
    if (partition.size != 1) {
      throw new IllegalArgumentException(
        s"Found ${partition.size} matching partitions. " +
          s"Expected exactly one partition to match the partition spec.")
    }
    // `CatalogStorageFormat.empty` over the case-class constructor: Spark 4.2 added a 7th
    // `serdeName` field, so a 6-arg call compiled against 4.2 emits `apply$default$7`, which does
    // not exist on 3.5/4.0/4.1. The factory is present on every supported version and yields the
    // same all-empty value.
    val dummyStorageFormat = CatalogStorageFormat.empty
    val statistics = partition.head
    // Include only reported values. Spark omits the "Partition Parameters" row for an empty map.
    val partParameters: Map[String, String] = Seq(
      PartitionStatistics.FIELD_FILE_COUNT -> statistics.fileCount(),
      PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES -> statistics.fileSizeInBytes(),
      PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME -> statistics.lastFileCreationTime(),
      PartitionStatistics.FIELD_RECORD_COUNT -> statistics.recordCount()
    ).collect {
      case (field, value) if PartitionStatistics.isKnown(value) => field -> value.toString
    }.toMap
    // CatalogTablePartition uses this temporary value only to render its "Partition Statistics"
    // row. It cannot represent an unknown size, so omit the row when size is unknown and omit an
    // unknown row count independently.
    val partStats = if (PartitionStatistics.isKnown(statistics.fileSizeInBytes())) {
      val rowCount =
        if (PartitionStatistics.isKnown(statistics.recordCount())) {
          Some(BigInt(statistics.recordCount()))
        } else {
          None
        }
      Some(CatalogStatistics(statistics.fileSizeInBytes(), rowCount))
    } else {
      None
    }
    val partitionDetails = CatalogTablePartition(
      partitionSpec,
      dummyStorageFormat,
      partParameters,
      statistics.lastFileCreationTime(),
      -1,
      partStats).toLinkedHashMap
    // Spark formats every createTime as a date; replace an unknown timestamp to avoid a 1969 date.
    if (!PartitionStatistics.isKnown(statistics.lastFileCreationTime())) {
      partitionDetails.put(
        PaimonDescribeTableExec.CREATED_TIME_KEY,
        PartitionStatisticsDisplay.UNKNOWN)
    }
    partitionDetails.foreach(s => rows += toCatalystRow(s._1, s._2, ""))
    rows += emptyRow()
  }

  private def addNotNullMetadata(schema: StructType, rows: ArrayBuffer[InternalRow]): Unit = {
    if (schema.fields.exists(!_.nullable)) {
      rows += toCatalystRow("# Column Not Null", "", "")
      schema.fields.filter(!_.nullable).foreach {
        column => rows += toCatalystRow(column.name, column.dataType.simpleString, "NOT NULL")
      }
    }
  }

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")
}

object PaimonDescribeTableExec {
  // Key for the partition creation-time row emitted by CatalogTablePartition.toLinkedHashMap.
  val CREATED_TIME_KEY = "Created Time"

  // This column metadata indicates the default value associated with a particular table column that
  // is in effect at any given time. Its value begins at the time of the initial CREATE/REPLACE
  // TABLE statement with DEFAULT column definition(s), if any. It then changes whenever an ALTER
  // TABLE statement SETs the DEFAULT. The intent is for this "current default" to be used by
  // UPDATE, INSERT and MERGE, which evaluate each default expression for each row.
  val CURRENT_DEFAULT_COLUMN_METADATA_KEY = "CURRENT_DEFAULT"
}
